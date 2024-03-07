from unittest import TestCase
from unittest.mock import Mock

import chispa
from pyspark.sql import SparkSession

from commons.configurations.configurator import Configurator
from enrichers.db_schema_equalizer import DbSchemaEqualizer

NO_TARGET_TABLE = ''


class TestDbSchemaEqualizer(TestCase):
    spark = SparkSession.builder.getOrCreate()

    def setUp(self) -> None:
        arks = {'query.return_value': [('col1',)]}
        empty_arks = {'query.return_value': []}
        self.query_runner_with_empty_result = Mock(**empty_arks)
        self.query_runner_with_non_empty_result = Mock(**arks)
        self.context = {}
        self.config = Configurator().get_configurator()

    def test_enricher_drops_column_not_in_schema(self):
        # Arrange
        config = self.config
        config.job.ingestion.general.target_table = NO_TARGET_TABLE
        config.job.ingestion.enrichers.track_db_schema = True

        db_schema_equalizer = DbSchemaEqualizer(config, self.query_runner_with_non_empty_result)

        df = self.spark.createDataFrame([[1, 2]], ['col1', 'col2'])

        # Act
        actual = db_schema_equalizer.enrich(df, self.context)

        # Assert
        expected = df.drop('col2')
        chispa.assert_df_equality(actual, expected, ignore_nullable=True)

    def test_no_enrich(self):
        # Arrange
        config = self.config
        config.job.ingestion.general.target_table = NO_TARGET_TABLE
        config.job.ingestion.enrichers.track_db_schema = False
        db_schema_equalizer = DbSchemaEqualizer(self.config, self.query_runner_with_non_empty_result)

        df = self.spark.createDataFrame([[1, 2]], ['col1', 'col2'])

        # Act
        actual = db_schema_equalizer.enrich(df, self.context)

        # Assert
        expected = df
        chispa.assert_df_equality(actual, expected, ignore_nullable=True)

    def test_equal_schemas(self):
        config = self.config
        # Arrange
        config.job.ingestion.general.target_table = NO_TARGET_TABLE
        config.job.ingestion.enrichers.track_db_schema = True

        db_schema_equalizer = DbSchemaEqualizer(config, self.query_runner_with_non_empty_result)

        df = self.spark.createDataFrame([[1]], ['col1'])

        # Act
        actual = db_schema_equalizer.enrich(df, self.context)

        # Assert
        expected = df
        chispa.assert_df_equality(actual, expected, ignore_nullable=True)

    def test_empty_df(self):
        config = self.config
        # Arrange
        config.job.ingestion.general.target_table = NO_TARGET_TABLE
        config.job.ingestion.enrichers.track_db_schema = True

        db_schema_equalizer = DbSchemaEqualizer(config, self.query_runner_with_empty_result)

        df = self.spark.createDataFrame([[1, 2]], ['col1', 'col2'])

        # Act
        actual = db_schema_equalizer.enrich(df, self.context)

        # Assert
        expected = df.drop('col1', 'col2')
        chispa.assert_df_equality(actual, expected, ignore_nullable=True)
