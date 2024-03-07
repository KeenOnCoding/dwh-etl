from unittest import TestCase


from commons.configurations.configurator import Configurator
from commons.utils.validators import RequireNotNoneException
from ingestion.query_params_builders.empty_query_param_builder \
    import EmptyQueryParamsBuilder
from ingestion.query_params_builders.query_params_builder_factory \
    import QueryParamsBuilderFactory


class TestQueryParamsBuilderFactory(TestCase):

    def setUp(self) -> None:
        self.config = Configurator().get_configurator()

    def test_snapshot_build(self):
        config = self.config
        actual = QueryParamsBuilderFactory(config).create()
        self.assertIsInstance(actual, EmptyQueryParamsBuilder)

    def test_incremental_build_no_required_parameters_set(self):
        config = self.config
        config.job.ingestion.fetching_strategy.fetching_strategy = 'INCREMENTAL'
        with self.assertRaises(RequireNotNoneException):
            QueryParamsBuilderFactory(config).create()

    def test_none_build(self):
        config = self.config
        config.job.ingestion.fetching_strategy.fetching_strategy = None
        with self.assertRaises(RequireNotNoneException):
            QueryParamsBuilderFactory(config).create()

    def test_wrong_build(self):
        config = self.config
        config.job.ingestion.fetching_strategy.fetching_strategy = 'WRONG_FETCHING_STRATEGY'

        with self.assertRaises(RuntimeError):
            QueryParamsBuilderFactory(config).create()
