from unittest import TestCase

import chispa
from pyspark.sql import SparkSession

from commons.configurations.configurator import Configurator
from enrichers.list_parser_enricher import ListParserEnricher


class TestListParserEnricher(TestCase):

    spark = SparkSession.builder.getOrCreate()

    def setUp(self) -> None:
        self.config = {}
        self.context = {}
        self.config = Configurator().get_configurator()

    def test_parse_single_column(self):
        config = self.config
        config.job.ingestion.enrichers.columns_to_parse = ['col1']

        # Arrange
        list_column_enricher = ListParserEnricher(config)
        df = self.spark.createDataFrame([[['test1', 'test2']]], ['col1'])

        # Act
        actual = list_column_enricher.enrich(df, self.context)

        # Assert
        expected = self.spark.createDataFrame([['test1,test2']], ['col1'])
        chispa.assert_df_equality(actual, expected)

    def test_parse_single_value_column(self):
        config = self.config
        config.job.ingestion.enrichers.columns_to_parse = ['col1']

        # Arrange
        list_column_enricher = ListParserEnricher(config)
        df = self.spark.createDataFrame([['test']], ['col1'])

        # Act
        actual = list_column_enricher.enrich(df, self.context)

        # Assert
        expected = df
        chispa.assert_df_equality(actual, expected)

    def test_parse_multiple_column(self):
        config = self.config
        config.job.ingestion.enrichers.columns_to_parse = ['col1','col2']

        # Arrange
        list_column_enricher = ListParserEnricher(config)
        df = self.spark.createDataFrame([[['test1', 'test2'], ['val1', 'val2']]], ['col1', 'col2'])

        # Act
        actual = list_column_enricher.enrich(df, self.context)

        # Assert
        expected = self.spark.createDataFrame([['test1,test2', 'val1,val2']], ['col1', 'col2'])
        chispa.assert_df_equality(actual, expected)

    def test_parse_wrong_column(self):
        config = self.config
        config.job.ingestion.enrichers.columns_to_parse = ['col2']

        # Arrange
        list_column_enricher = ListParserEnricher(config)
        df = self.spark.createDataFrame([['test']], ['col1'])

        # Act
        actual = list_column_enricher.enrich(df, self.context)

        # Assert
        expected = df
        chispa.assert_df_equality(actual, expected)

    def test_parse_no_column(self):
        config = self.config
        config.job.ingestion.enrichers.columns_to_parse = None

        # Arrange
        list_column_enricher = ListParserEnricher(config)
        df = self.spark.createDataFrame([['test']], ['col1'])

        # Act
        actual = list_column_enricher.enrich(df, self.context)

        # Assert
        expected = df
        chispa.assert_df_equality(actual, expected)
