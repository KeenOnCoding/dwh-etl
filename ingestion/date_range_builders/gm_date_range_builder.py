from datetime import datetime, date

from ingestion.date_range_builders.date_range_builder import IncrementalPayloadBuilder
from ingestion.date_range_builders.date_operations import add_offset

from commons.logging.simple_logger import logger
from ingestion import constants


class GmIncrementalPayloadBuilder(IncrementalPayloadBuilder):
    """
        This class implements custom logic for building date range
        for GM Extended Summary.

        As report stakeholders need data being updated on daily
        basis for previous month till the next 12 months, we
        implemented this class to perform custom ingestion operation

        Dates are counted as
        date_from = now + date_from_time_offset
        date_to = now + 11 months

        In addition to the ColumnDateRangeBuilder this class can
        automatically detect a data lag and increase the batch window
        size and time offset to process all the missed data
    """

    DEFAULT_BATCH_WINDOW = 12
    DEFAULT_GRANULARITY = constants.MONTHS
    DEFAULT_DATE_FROM_TIME_OFFSET = -1

    def __init__(self, config, query_runner):
        IncrementalPayloadBuilder.__init__(self, config, query_runner)

    def batch_window(self):
        return IncrementalPayloadBuilder.batch_window(self) or self.DEFAULT_BATCH_WINDOW

    def granularity(self):
        return IncrementalPayloadBuilder.batch_window(self) or self.DEFAULT_GRANULARITY

    def date_from_time_offset(self):
        return IncrementalPayloadBuilder.date_from_time_offset(self) or self.DEFAULT_DATE_FROM_TIME_OFFSET

    def build(self):
        """ Returns inclusive range [from;to] """

        date_from = datetime.fromordinal(date.today().toordinal())
        batch_window = self.batch_window() + abs(self.date_from_time_offset())
        date_from_time_offset = self.date_from_time_offset()

        date_from = add_offset(date_from,
                               date_from_time_offset,
                               self.batch_window_unit())

        date_to = add_offset(date_from,
                             batch_window,
                             self.batch_window_unit())

        logger.info(f'Generated date range: '
                    f'date_from = {date_from}, '
                    f'date_to = {date_to}')

        return date_from, date_to

    def __int_to_str_with_sign(self, num):
        return f"+{num}" if num >= 0 else str(num)
