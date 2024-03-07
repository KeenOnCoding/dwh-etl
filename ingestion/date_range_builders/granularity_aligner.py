from ingestion.date_range_builders.date_range_builder import IncrementalPayloadBuilder

from commons.logging.simple_logger import logger
from ingestion.date_range_builders import date_operations
from ingestion import constants


class GranularityAligner(IncrementalPayloadBuilder):
    DEFAULT_GRANULARITY = constants.HOURS

    def __init__(self, config, date_range_builder):
        IncrementalPayloadBuilder.__init__(self, config)
        self.date_range_builder = date_range_builder

    def granularity(self):
        return IncrementalPayloadBuilder.granularity(self) or self.DEFAULT_GRANULARITY

    def build(self):
        modification_date_from, modification_date_to = self.date_range_builder.build()

        align_dates = self.config.job.ingestion.fetching_strategy.align_dates

        if align_dates:
            modification_date_from = date_operations.align_date(modification_date_from, self.granularity())
            modification_date_to = date_operations.align_date(modification_date_to, self.granularity())
        else:
            pass

        logger.info(f'Granulated date range: '
                    f'date_from = {modification_date_from}, '
                    f'date_to = {modification_date_to}')

        return modification_date_from, modification_date_to
