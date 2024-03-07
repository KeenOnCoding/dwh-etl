from datetime import datetime
from typing import List

from commons.logging.simple_logger import logger
from commons.utils.stringconverters import dhrms_string_to_timedelta
from ingestion.constants import DEFAULT_DATETIME_PATTERN
from ingestion.date_range_builders.date_operations import add_offset, align_date
from ingestion.date_range_builders.date_operations import increment_by_unit
from ingestion.date_range_builders.date_range_builder import IncrementalPayloadBuilder


class BackfillIncrementalPayloadBuilder(IncrementalPayloadBuilder):
    DEFAULT_BACKFILL_UPPER_OFFSET = 0
    DEFAULT_DATE_FROM_TIME_OFFSET = 0
    DEFAULT_GRANULARITY = 'minutes'

    def __init__(self, config, query_runner):
        IncrementalPayloadBuilder.__init__(self, config, query_runner)

    def batch_window(self):
        return IncrementalPayloadBuilder.batch_window(self)

    def backfill_upper_offset(self):
        return self.config.job.ingestion.fetching_strategy.backfill_upper_offset or self.DEFAULT_BACKFILL_UPPER_OFFSET

    def date_from_time_offset(self):
        return IncrementalPayloadBuilder.date_from_time_offset(self) or self.DEFAULT_DATE_FROM_TIME_OFFSET

    def granularity(self):
        return IncrementalPayloadBuilder.granularity(self) or self.DEFAULT_GRANULARITY

    def build(self) -> List[tuple]:
        if int(self.batch_window()) < 0:
            raise ValueError(f'{self.config.job.ingestion.fetching_strategy.batch_window} should not be negative')
        using_default_date, latest_found_date = self.read_latest_date()
        latest_date = align_date(latest_found_date, self.granularity())

        time_now = align_date(datetime.now(), self.granularity())

        upper_time_bound = add_offset(time_now, self.backfill_upper_offset(), self.batch_window_unit())

        datetime_to = add_offset(latest_date, self.date_from_time_offset() if not using_default_date else 0,
                                 self.batch_window_unit())

        date_ranges = []

        if self.config.job.ingestion.fetching_strategy.backfill_safe_window:
            safe_window = self.config.job.ingestion.fetching_strategy.backfill_safe_window
            upper_time_bound -= dhrms_string_to_timedelta(safe_window)
            logger.info(f'Safe window from now is: {safe_window} [{upper_time_bound}]')

        # if calculated upper bound is before or equal to the latest date (both granulated)
        # then we should skip the backfill cycle to prevent fetching the periods
        # that we already have
        if upper_time_bound <= datetime_to:
            return []

        backfill_max_ranges_per_run = self.config.job.ingestion.fetching_strategy.backfill_max_ranges_per_run

        iteration = 0
        while True:
            if iteration == backfill_max_ranges_per_run:
                break

            if self.config.job.ingestion.fetching_strategy.backfill_left_bound_kick_forward:
                if not (iteration == 0 and using_default_date):
                    datetime_to += dhrms_string_to_timedelta(
                        self.config.job.ingestion.fetching_strategy.backfill_left_bound_kick_forward)

            datetime_from = datetime_to
            datetime_to = align_date(add_offset(datetime_from, self.batch_window(), self.batch_window_unit()),
                                     self.granularity())

            if datetime_from > datetime_to or upper_time_bound < datetime_to:
                if self.config.job.ingestion.fetching_strategy.backfill_crop_now:
                    logger.info(f'BACKFILL_CROP_NOW is enabled. Adding additional cropped range.')
                    logger.info(f'Generated backfill range: '
                                f'date_from = {datetime_from}, '
                                f'date_to = {upper_time_bound}')
                    date_ranges.append((datetime_from, upper_time_bound))
                break

            logger.info(f'Generated backfill range: '
                        f'date_from = {datetime_from}, '
                        f'date_to = {datetime_to}')

            date_ranges.append((datetime_from, datetime_to))
            # add default delta to the to time to prevent infinity loop
            if datetime_to == datetime_from:
                datetime_to = increment_by_unit(datetime_to, self.batch_window_unit())

            iteration += 1

        return date_ranges

    def read_latest_date(self):
        using_default_date = False
        date = self.query_runner \
            .query_single_result(f"select max({self.date_column()}) "
                                 f"from {self.target_table()}")

        if date is None:
            default_value = self.default_partition_value()
            using_default_date = True
            if default_value is None:
                raise ValueError(
                    f"{self.config.job.ingestion.fetching_strategy.default_partition_value} is not set. "
                )
            logger.info("Target table is empty. "
                        f"Using default partition value as {default_value}")

            date = datetime.strptime(
                default_value,
                DEFAULT_DATETIME_PATTERN
            )
        logger.info(f'Found latest date: {date}')
        return using_default_date, date
