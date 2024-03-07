from commons.logging.simple_logger import logger
from ingestion.date_range_builders.date_range_builder_factory import IncrementalPayloadBuilderFactory
from ingestion.exec.batch import BatchIdProvider
from ingestion.exec.dummy_exec_context import DummyExecutionContext
from ingestion.exec.incremental_exec_context import IncrementalExecutionContext
from ingestion.exec.snapshot_exec_context import SnapshotExecutionContext
from commons.utils.validators import require_not_none

from ingestion import constants


class ExecutionContextBuilder:
    def __init__(self, *, config, query_runner):
        self.config = config
        self.query_runner = query_runner

        self.granularity = self.config.job.ingestion.fetching_strategy.granularity or constants.HOURS

    def build(self):
        fetching_strategy = require_not_none(self.config.job.ingestion.fetching_strategy.fetching_strategy)

        logger.info(f'Using {fetching_strategy} fetching strategy')

        if fetching_strategy == 'SNAPSHOT':
            return [SnapshotExecutionContext(BatchIdProvider.get_batch_id())]

        elif fetching_strategy == 'INCREMENTAL':
            date_range_builder = IncrementalPayloadBuilderFactory(
                self.config,
                self.query_runner).create()
            logger.info(f"Using '{type(date_range_builder).__name__}' IncrementalPayloadBuilderFactory")
            date_ranges = date_range_builder.build()
            contexts = []

            for date_range in date_ranges:
                batch_id = BatchIdProvider.get_batch_id()

                contexts.append(
                    IncrementalExecutionContext(
                        batch_id=batch_id,
                        date_range=date_range,
                    )
                )
            return contexts
        elif fetching_strategy == 'WEEK-YEAR':
            return [DummyExecutionContext(BatchIdProvider.get_batch_id())]

        raise RuntimeError(f'FETCHING_STRATEGY is incorrect: {fetching_strategy}')
