from commons.logging.simple_logger import logger
from commons.spark.spark_db_sink import AbstractDatabaseSink
from ingestion.constants import OUT_TABLE
from ingestion.data_sources.datasource import AbstractDataSource
from ingestion.enrichers.enricher_chain import EnricherChain
from ingestion.pipeline.core import Operation
from ingestion.pipeline.core import Pipeline
from ingestion.pipeline.operation.core import StashToContextOperation, PopFromContextOperation


class CountOperation(Operation):

    def _invoke(self, df, context):
        return df.count()


class CountToContext(Operation):
    def __init__(self, key, *, force=False):
        self.pipeline = Pipeline() \
            .register(StashToContextOperation("data", force=force)) \
            .register(CountOperation()) \
            .register(StashToContextOperation(key, force=force)) \
            .register(PopFromContextOperation("data"))

    def _invoke(self, payload, context):
        is_err, res = self.pipeline.invoke(payload=payload, context=context)
        if is_err:
            raise RuntimeError('Error occur when counting to context') from res

        return res


class ExtractDataOperation(Operation):
    def __init__(self, data_source: AbstractDataSource):
        self.data_source = data_source

    def _invoke(self, payload, context):
        logger.info("Extraction step")
        logger.info(f"Using '{type(self.data_source).__name__}' datasource")
        return self.data_source.extract_data(context)


class EnrichDataOperation(Operation):
    def __init__(self, enricher_chain: EnricherChain):
        self.enricher_chain = enricher_chain

    def _invoke(self, df, context):
        logger.info("Enrichment step")
        logger.info(f"Invoke with context: {context}")
        return self.enricher_chain.enrich(df, context)


class LoadDataOperation(Operation):

    def __init__(self, sink: AbstractDatabaseSink):
        self.sink = sink

    def _invoke(self, df, context):
        logger.info(f"Loading step: load data to {context[OUT_TABLE]} table")
        self.sink.save(df, context[OUT_TABLE])
