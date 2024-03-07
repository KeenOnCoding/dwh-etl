import time
from datetime import date, timedelta, datetime
import calendar
from pyspark.sql.functions import lit
from commons.db.query_runner import QueryRunner
from crossrates_to_usd import CrossRateToUsd

crossrates_table_schema = ['currency', 'date', 'value']


class RiksBankCrossRatesRunner:

    def __init__(self, obs_url, cross_url, default_date,
                 latest_table, spark_session, target_table, spark_db_sink,
                 sleep_duration, query_config, query_engine, currency_codes_source_table, build_number, build_url):
        self.obs_url = obs_url
        self.cross_url = cross_url
        self.default_date = default_date
        self.latest_table = latest_table
        self.spark_session = spark_session
        self.target_table = target_table
        self.spark_db_sink = spark_db_sink
        self.sleep_duration = sleep_duration
        self.query_config = query_config
        self.query_engine = query_engine
        self.currency_codes_source_table = currency_codes_source_table
        self.build_number = build_number
        self.build_url = build_url

    def run(self) -> None:

        time_now = datetime.now()
        today = date.today()

        max_date = QueryRunner(self.query_config, self.query_engine) \
            .query(f"""select MAX([date]) from {self.latest_table}""")

        max_date = max_date[0] if max_date else self.default_date
        threshold_date = today - timedelta(days=7)

        if max_date is not None and max_date[0] is None:
            offset_date = self.default_date
        else:
            offset_date = max_date[0]

        if isinstance(offset_date, datetime):
            offset_date = offset_date.date()
        elif isinstance(offset_date, str):
            offset_date = datetime.strptime(offset_date, "%Y-%m-%d").date()

        date_from = min(threshold_date, offset_date).strftime("%Y-%m-%d")

        # SELECT CURRENCY CODES FROM DWH
        keys = QueryRunner(self.query_config, self.query_engine)\
            .query(f"""select distinct(short_description) from 
                {self.currency_codes_source_table} where series_closed = 0""")

        # from tuple to list
        keys = [item[0].strip() for item in keys]

        crossrates = CrossRateToUsd(obs_url=self.obs_url,
                                    cross_url=self.cross_url,
                                    date_from=date_from,
                                    keys=keys,
                                    sleep_duration=self.sleep_duration)\
            .prepare_data_to_schema()



        # timestamp value for baych_id column
        current_gmt = time.gmtime()
        timestamp = calendar.timegm(current_gmt)

        stage_table = self.spark_session\
            .createDataFrame(data=crossrates, schema=crossrates_table_schema)\
            .withColumn("batch_id", lit(timestamp))

        try:
            self.spark_db_sink.save(data=stage_table, table_name=self.target_table)

            QueryRunner(self.query_config, self.query_engine)\
                .execute(query=f"""INSERT INTO dbo.util_batch_track_table
                            ([date], table_name, max_batch, min_batch, is_processed, 
                            ingestion_build_number, ingestion_build_url,
                            processor_build_number, processor_build_url)
                        VALUES('{time_now}', '{self.target_table}', {timestamp}, {timestamp}, 0,
                            {self.build_number}, '{self.build_url}', 0, '');
                        """)

        except RuntimeError as error:
            print(f"{error}")
