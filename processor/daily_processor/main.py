import pyspark.sql.functions as F

from datetime import timedelta
from pyspark.sql.functions import udf
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, TimestampType

from commons.db.query_runner import QueryRunner
from commons.logging.simple_logger import logger
from processor.daily_processor.time_interval import TimeInterval

from processor.daily_processor import config as cfg


def split_to_daily(date1, date2, overtime, weekends):
    """udf of spliting start and finish date tuples to daily"""
    dates = []

    # skip spliting in case of overtime on weekends
    if date1.isoweekday() < 6 and overtime is False:

        # get period of report record
        delta_days = (date2.date() - date1.date()).days + 1
        for i in range(delta_days):
            current_date = date1 + timedelta(days=i)
            # exclude weekends
            if current_date.isoweekday() < 6 and current_date not in weekends:
                dates.append((current_date, current_date))
    else:
        dates.append((date1, date2))

    return dates


split_to_daily_udf = udf(split_to_daily, ArrayType(ArrayType(TimestampType())))


def split_data(records: DataFrame, weekends: list):
    started = F.col("started")
    finished = F.col("finished")

    litted_weekends = [(lambda x: F.lit(x))(x) for x in weekends]
    return records \
        .withColumn("started",
                    split_to_daily_udf(started,  # split dates: for example, if 40 hours were reported in one record
                                       finished,  # it will be split in 5 equal records
                                       F.col("overtime"),
                                       F.array(litted_weekends))) \
        .withColumn("effort", F.col("effort") / F.size(started)) \
        .withColumn("started", F.explode(started)) \
        .withColumn("finished", started[1]) \
        .withColumn("started", started[0]) \
        .withColumn("started_date", F.col("started").cast("date")) \
        .withColumn("finished_date", F.col("finished").cast("date"))


class DailyProcessorRunner:

    def __init__(self, config, engine, data_source, spark_db_sink, query_runner: QueryRunner):
        self.config = config
        self.target_table = config.job.processing.daily_processor.daily_proc_target_table
        self.data_source = data_source
        self.engine = engine
        self.spark_db_sink = spark_db_sink
        self.query_runner = query_runner
        self.date_column = config.job.processing.daily_processor.date_column

    def run(self, source_table, expected_calendar_time_table):
        time = TimeInterval(self.config)
        logger.info(f"Using  '{source_table}' as source table")
        logger.info(f"Using '{expected_calendar_time_table}' as expected calendar table")
        logger.info(f"Using '{self.target_table}' as target table")

        min_date, max_date = time.get_dates_to_fetch(self.query_runner, self.target_table, self.date_column)
        logger.info(f"Using range to fetch: {min_date} - {max_date}")

        data: DataFrame = self.data_source.extract_data(source_table,
                                                        [f"status = 'Accepted' "
                                                         f"AND started >= '{min_date}' AND finished <= '{max_date}' "
                                                         f"AND deleted = 0"])
        data.persist()

        if data.count() == 0:
            logger.info("No new data to process")
            return

        logger.info(f"Time reports date range (min-started/max-finished is: [{min_date}, {max_date}]")

        expected_calendars_daily: DataFrame = self.data_source.extract_data(  # Kyiv calendar
            expected_calendar_time_table,
            [f"date >= '{min_date}' and date <= '{max_date}' and calendar_id = '{cfg.CALENDAR_ID}'"]
        )

        if expected_calendars_daily.count() == 0:
            raise RuntimeError(f"No data in {expected_calendar_time_table}."
                               f"Got interval with start date [{min_date}]"
                               f" and finish date [{max_date}]")

        weekends = [row[0] for row in
                    expected_calendars_daily.where(F.col('working_hours') == 0).select(F.col('date')).collect()]
        logger.info(f'Captured weekends: {weekends}')

        split_records_by_weekends = split_data(data, weekends).persist()
        logger.info(f'Amount of splitted records: {split_records_by_weekends.count()}')

        self.query_runner.execute(f"DELETE FROM {self.target_table} WHERE {self.date_column} "
                                  f"BETWEEN '{min_date}' AND '{max_date}'")

        self.spark_db_sink.save(split_records_by_weekends, self.target_table)
        logger.info(f"Saved {split_records_by_weekends.count()} record to table")
