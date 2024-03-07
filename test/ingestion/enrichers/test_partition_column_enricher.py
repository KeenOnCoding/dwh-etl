from unittest import TestCase

import chispa
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from commons.configurations.configurator import Configurator
from ingestion.constants import BATCH_ID
from ingestion.enrichers.partition_column_enricher import PartitionColumnEnricher


class TestPartitionColumnEnricher(TestCase):
    spark = SparkSession.builder.getOrCreate()

    def setUp(self) -> None:
        self.config = Configurator().get_configurator()
        self.config.job.ingestion.general.partition_columns = 'version'

    def test_enrich(self):
        # Arrange
        partition_column_enricher = PartitionColumnEnricher(self.config)

        schema = StructType([])
        df = self.spark.createDataFrame([], schema)

        # Act
        actual = partition_column_enricher.enrich(df, {BATCH_ID: 100})

        # Assert
        expected = df.withColumn("version", F.lit(100))
        chispa.assert_df_equality(actual, expected, ignore_nullable=True)
