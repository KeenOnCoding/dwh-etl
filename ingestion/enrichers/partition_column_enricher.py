import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from commons.logging.simple_logger import logger
from ingestion.constants import BATCH_ID
from ingestion.enrichers.abstract_enricher import AbstractEnricher


class PartitionColumnEnricher(AbstractEnricher):
    """
    PartitionColumnEnricher is used to append PARTITION_COLUMN
    with literal value from the supplied context under BATCH_ID key
    """

    def __init__(self, config):
        self.config = config

    def enrich(self, df: DataFrame, context):
        column_name = self.config.job.ingestion.general.partition_columns
        logger.info(f"Using PartitionColumnEnricher by {column_name} column")
        return df.withColumn(column_name, F.lit(context[BATCH_ID]))
