from pyspark.sql import DataFrame
from pyspark.sql.functions import to_json

from commons.logging.simple_logger import logger
from ingestion.enrichers.abstract_enricher import AbstractEnricher


class JsonStringifyEnricher(AbstractEnricher):
    """
    JsonStringifyEnricher transforms nested structure to json string
    """

    def __init__(self, config):
        self.columns_to_stringify = config.job.ingestion.enrichers.stringify_json_columns

    def enrich(self, df: DataFrame, context) -> DataFrame:
        if self.columns_to_stringify is not None:
            logger.info(f"Using JsonStringifyEnricher for transform nested structure to json string"
                        f" by '{self.columns_to_stringify}' columns")
            for column_name in self.columns_to_stringify:
                self.raise_on_column_absence(df, column_name)
                df = df.withColumn(column_name, to_json(column_name))

        return df
