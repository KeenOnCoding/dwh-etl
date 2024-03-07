import chispa

from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField

from commons.configurations.configurator import Configurator
from ingestion.enrichers.field_mappings import FieldMappingsEnricher


class TestFieldMappings(TestCase):
    spark = SparkSession.builder.getOrCreate()

    def setUp(self) -> None:
        self.context = {}
        self.config = Configurator().get_configurator()

    def test_none_enrich(self):
        # Arrange
        config = self.config
        config.job.ingestion.enrichers.field_mapping = None
        field_mappings_enricher = FieldMappingsEnricher(config)

        schema = StructType([StructField("col1", StringType()), StructField("col2", StringType())])
        df = self.spark.createDataFrame([], schema)

        # Act
        actual = field_mappings_enricher.enrich(df, self.context)

        # Assert
        chispa.assert_df_equality(actual, df)

    def test_mapping_enrich(self):
        # Arrange
        config = self.config
        field_mapping = [
            {
                "from": "col1",
                "to": "test"
            }
        ]

        config.job.ingestion.enrichers.field_mapping = field_mapping
        field_mappings_enricher = FieldMappingsEnricher(self.config)

        schema = StructType([StructField("col1", StringType()), StructField("col2", StringType())])
        df = self.spark.createDataFrame([], schema)

        # Act
        actual = field_mappings_enricher.enrich(df, self.context)

        # Assert
        expected = df.withColumnRenamed("col1", "test")
        chispa.assert_df_equality(actual, expected)
