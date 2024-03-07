from datetime import datetime

from commons.logging.simple_logger import logger
from ingestion import constants
from ingestion.date_range_builders.date_operations import add_offset
from ingestion.date_range_builders.date_range_builder import IncrementalPayloadBuilder


class UpperBoundaryChecker(IncrementalPayloadBuilder):
    """Checks if dateRange exceeds set upper boundaries"""
    DEFAULT_GRANULARITY = constants.HOURS

    def __init__(self, config, date_range_builder):
        IncrementalPayloadBuilder.__init__(self, config)
        self.date_range_builder = date_range_builder

    def granularity(self):
        return IncrementalPayloadBuilder.granularity(self) or self.DEFAULT_GRANULARITY

    def build(self):
        """Returns inclusive range [from;to]"""

        date_range = self.date_range_builder.build()

        modification_date_from, modification_date_to = date_range

        date_range_upper_bound = self.config.job.ingestion.fetching_strategy.date_range_upper_bound

        granularity = self.batch_window_unit()

        modification_date_to = min(modification_date_to,
                                   add_offset(datetime.now(),
                                              date_range_upper_bound,
                                              granularity))

        logger.info(f'Modified date range: '
                    f'date_from = {modification_date_from}, '
                    f'date_to = {modification_date_to}')

        return modification_date_from, modification_date_to
