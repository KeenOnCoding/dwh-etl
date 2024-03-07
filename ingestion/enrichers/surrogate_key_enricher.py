from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from commons.logging.simple_logger import logger
from ingestion.enrichers.abstract_enricher import AbstractEnricher


class SurrogateKeyEnricher(AbstractEnricher):

    def __init__(self, config):
        self.surrogate_key_column = config.job.ingestion.enrichers.surrogate_key_columns

    def enrich(self, df: DataFrame, context):
        if not self.surrogate_key_column:
            logger.info("No surrogate key colum")
            return df
        logger.info(f"Using SurrogateKeyEnricher with surrogate key column: {self.surrogate_key_column}")
        return df.withColumn(self.surrogate_key_column, expr('uuid()'))
