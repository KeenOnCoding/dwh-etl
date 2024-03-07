from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import TimestampType

from commons.logging.simple_logger import logger
from ingestion.enrichers.abstract_enricher import AbstractEnricher


class ToDatetimeEnricher(AbstractEnricher):
    """
    IsoDateAligner is used to cast date column to the ISO date format
    """

    def __init__(self, config):
        self.date_column = config.job.ingestion.enrichers.date_column_to_datetime
        self.date_column_origin_format = config.job.ingestion.enrichers.date_column_origin_format

        if (self.date_column is None and self.date_column_origin_format) or (
                self.date_column_origin_format is None and self.date_column):
            raise RuntimeError(f'Either DATE_COLUMN_TO_DATETIME or DATE_COLUMN_ORIGIN_FORMAT is unfilled.')

    def enrich(self, df: DataFrame, context) -> DataFrame:
        if not all(v is not None for v in [self.date_column, self.date_column_origin_format]):
            return df

        logger.info(f"Using ToDatetimeEnricher for cast date column '{self.date_column}' to the ISO date format")
        self.raise_on_column_absence(df, self.date_column)

        to_iso = udf(
            lambda x: datetime.strptime(x, self.date_column_origin_format),
            TimestampType()
        )

        return df.withColumn(
            self.date_column,
            to_iso(df[self.date_column]).alias(self.date_column)
        )
