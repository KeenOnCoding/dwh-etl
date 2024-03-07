from commons.logging.simple_logger import logger
from ingestion.constants import IN_AMOUNT, OUT_AMOUNT
from ingestion.pipeline.core import Pipeline
from ingestion.pipeline.operation.etl import CountToContext
from ingestion.pipeline.operation.etl import EnrichDataOperation
from ingestion.pipeline.operation.etl import ExtractDataOperation
from ingestion.pipeline.operation.etl import LoadDataOperation


class EtlPipelineBuilder:
    def __init__(self, *, sink, enricher_chain, datasource):
        self.sink = sink
        self.enricher_chain = enricher_chain
        self.datasource = datasource

    def build(self):
        logger.info("ETL pipeline building")
        pipeline = Pipeline() \
            .register(ExtractDataOperation(self.datasource)) \
            .register(CountToContext(IN_AMOUNT, force=True)) \
            .register(EnrichDataOperation(self.enricher_chain)) \
            .register(CountToContext(OUT_AMOUNT, force=True)) \
            .register(LoadDataOperation(self.sink))

        return pipeline
