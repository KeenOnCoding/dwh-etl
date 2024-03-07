import datetime
from datetime import timedelta
from dateutil import parser, relativedelta
from commons.logging.simple_logger import logger


class TimeInterval:
    def __init__(self, config):
        self.config = config

    def get_latest_date(self, query_runner, target_table, date_column):
        using_default_date = False
        date = query_runner.query_single_result(f"""select max({date_column}) from {target_table}""")
        if date is None:
            default_value = self.config.job.processing.daily_processor.default_partition_value
            if default_value is None:
                raise ValueError(
                    f"DEFAULT_PARTITION_VALUE is not set. "
                )
            using_default_date = True
            logger.info("Target table is empty. "
                        f"Using default partition value as {default_value}")
            date = parser.parse(default_value)
        logger.info(f'Chosen latest date: {date} in {target_table}')
        return date, using_default_date

    def get_end_date(self, start_date):
        batch_window = self.config.job.processing.daily_processor.batch_window
        logger.info(f"Using batch window '{batch_window}'")
        end_date = start_date + relativedelta.relativedelta(months=batch_window) - timedelta(days=1)
        return end_date

    def get_dates_to_fetch(self, query_runner, target_table, date_column):
        min_date, using_default_date = self.get_latest_date(query_runner, target_table, date_column)
        if not using_default_date:
            min_date += timedelta(days=1)
            start_of_current_month = datetime.date.today() + relativedelta.relativedelta(day=1)
            end_of_current_month = start_of_current_month + relativedelta.relativedelta(day=31)
            max_date = self.get_end_date(min_date)
            if end_of_current_month < max_date.date():
                previous_date = start_of_current_month + relativedelta.relativedelta(months=-1)
                return [previous_date, end_of_current_month]
        max_date = self.get_end_date(min_date)
        return [min_date, max_date]
