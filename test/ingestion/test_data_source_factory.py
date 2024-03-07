from unittest import TestCase

from configurations.configurator import Configurator
from ingestion import constants
from ingestion.data_sources.data_source_factory import DataSourceFactory
from ingestion.data_sources.http_data_source import HttpDataSource
from ingestion.data_sources.sr_http_data_source import SrHttpDataSource

NO_QUERY_RUNNER = None
NO_QUERY_PARAMS_BUILDER = None
NO_HASH_SUM_EXTRACTOR = None
NO_HASH_SUM_MANAGER = None
NO_SPARK = None
NO_TOKEN_SUPPLIER = None


class TestDataSourceFactory(TestCase):
    def setUp(self) -> None:
        self.config = Configurator().get_configurator()

    def test_generic_rest_data_source_build(self):
        config = self.config
        config.job.ingestion.data_source.data_source = constants.GENERIC_REST_DATASOURCE

        actual = DataSourceFactory(config, NO_QUERY_RUNNER,
                                   NO_QUERY_PARAMS_BUILDER,
                                   NO_HASH_SUM_EXTRACTOR,
                                   NO_SPARK, NO_TOKEN_SUPPLIER).create()

        self.assertIsInstance(actual, HttpDataSource)

    def test_sr_data_source_build(self):
        config = self.config
        config.job.ingestion.data_source.data_source = constants.SMART_RECRUITERS_DATASOURCE

        actual = DataSourceFactory(config, NO_QUERY_RUNNER,
                                   NO_QUERY_PARAMS_BUILDER,
                                   NO_HASH_SUM_EXTRACTOR,
                                   NO_SPARK, NO_TOKEN_SUPPLIER).create()

        self.assertIsInstance(actual, SrHttpDataSource)

    def test_wrong_data_source_build(self):
        config = self.config
        config.job.ingestion.data_source.data_source = "WRONG-DS"

        with self.assertRaises(Exception):
            DataSourceFactory(config, NO_QUERY_RUNNER, NO_QUERY_PARAMS_BUILDER,
                              NO_HASH_SUM_EXTRACTOR,
                              NO_SPARK, NO_TOKEN_SUPPLIER).create()

    def test_not_set_data_source_build(self):
        config = self.config

        with self.assertRaises(Exception):
            DataSourceFactory(config, NO_QUERY_RUNNER, NO_QUERY_PARAMS_BUILDER,
                              NO_HASH_SUM_EXTRACTOR,
                              NO_SPARK, NO_TOKEN_SUPPLIER).create()
