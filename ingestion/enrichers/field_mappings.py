from pyspark.sql import DataFrame

from ingestion.enrichers.abstract_enricher import AbstractEnricher
from commons.logging.simple_logger import logger


class FieldMappingsEnricher(AbstractEnricher):
    """Transformation class

    Is aimed to transform data coming from 1C rest
    end point and rename fields if needed,
    if cmd param `field-mapping` is emply then no mapping should happen
    """

    def __init__(self, config):
        self.field_mappings = config.job.ingestion.enrichers.field_mapping

    def enrich(self, df: DataFrame, context):
        logger.info(f"Using FieldMappingsEnricher: renaming columns: '{self.field_mappings}'")
        if self.field_mappings:
            for i in self.field_mappings:
                df = df.withColumnRenamed(i["from"], i["to"])
        return df
