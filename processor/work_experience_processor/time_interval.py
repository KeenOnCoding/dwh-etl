import datetime
from dateutil import relativedelta

from commons.logging.simple_logger import logger


class TimeInterval:
    def get_ranges(self, start_date, end_date):
        ranges = list()
        date = start_date + relativedelta.relativedelta(day=31)
        while date < end_date:
            ranges.append(date)
            date += relativedelta.relativedelta(months=1, day=31)
        logger.info(f"Dates range'{ranges[0]}' - '{ranges[-1]}'")
        return ranges

    def get_max_date(self, query_runner, source_table, target_table, date_column='date'):
        max_date = query_runner.query_single_result(f"""select max({date_column}) from {target_table}""")
        using_default_date = False
        if max_date is None:
            default_value = query_runner.query_single_result(f"""select min({date_column}) from {source_table}""")
            if default_value is None:
                raise ValueError(
                    f"Source table have no min date. "
                )
            using_default_date = True
            max_date = default_value
        logger.info(f"Using '{max_date}' as latest date in target table '{target_table}'")
        return max_date, using_default_date

    def get_interval_to_fetch(self, query_runner, source_table, target_table, date_column='date'):
        max_date, using_default_date = self.get_max_date(query_runner, source_table, target_table, date_column)
        if not using_default_date:
            start_date = max_date + relativedelta.relativedelta(months=1)
            end_date = start_date + relativedelta.relativedelta(months=6)
            date_for_current_month = TimeInterval.get_current_date() + relativedelta.relativedelta(day=31)
            if end_date > date_for_current_month:
                start_date = date_for_current_month - relativedelta.relativedelta(months=6)
                return start_date, date_for_current_month
            return start_date, end_date
        return max_date, max_date + relativedelta.relativedelta(months=6)

    @classmethod
    def get_current_date(cls):
        return datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
