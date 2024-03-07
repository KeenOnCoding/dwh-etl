from commons.logging.simple_logger import logger
from commons.utils.validators import require_not_none
from ingestion.date_range_builders.backfill_daterange_builder import BackfillIncrementalPayloadBuilder
from ingestion.date_range_builders.column_date_range_builder import ColumnIncrementalPayloadBuilder
from ingestion.date_range_builders.date_range_builder import IncrementalBuilderApiAdapter
from ingestion.date_range_builders.gm_date_range_builder import GmIncrementalPayloadBuilder
from ingestion.date_range_builders.granularity_aligner import GranularityAligner
from ingestion.date_range_builders.upper_boundary_checker import UpperBoundaryChecker


class IncrementalPayloadBuilderFactory:

    def __init__(self, config, query_runner):
        self.config = config
        self.query_runner = query_runner

    def create(self):
        incremental_strategy = require_not_none(self.config.job.ingestion.fetching_strategy.incremental_strategy,
                                                f'INCREMENTAL_STRATEGY must be set')

        logger.info(f'Using {incremental_strategy} incremental strategy')
        if incremental_strategy == "BACKFILL":
            return BackfillIncrementalPayloadBuilder(self.config, self.query_runner)

        elif incremental_strategy == "COLUMN-DATE":
            date_range_builder = ColumnIncrementalPayloadBuilder(self.config,
                                                                 self.query_runner)
            date_range_builder = UpperBoundaryChecker(self.config,
                                                      date_range_builder)
            return IncrementalBuilderApiAdapter(GranularityAligner(self.config, date_range_builder))

        elif incremental_strategy == "CUSTOM-GM":
            date_range_builder = GmIncrementalPayloadBuilder(self.config,
                                                             self.query_runner)
            date_range_builder = UpperBoundaryChecker(self.config,
                                                      date_range_builder)
            return IncrementalBuilderApiAdapter(GranularityAligner(self.config, date_range_builder))

        raise Exception('INCREMENTAL_STRATEGY is incorrect')
