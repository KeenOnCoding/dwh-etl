from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock

from commons.configurations.configurator import Configurator
from commons.utils.validators import RequireNotNoneException
from hash_sum_utils.hash_sum_manager import HashSumManager
from ingestion.exec.exec_context_builder import ExecutionContextBuilder
from ingestion.exec.incremental_exec_context import IncrementalExecutionContext


class TestExecutionContextBuilder(TestCase):
    def setUp(self) -> None:
        arks = {'query_single_result.return_value': datetime(2020, 12, 12)}
        self.query_runner_mock = Mock(**arks)

        self.config = Configurator().get_configurator()
        dummy_arks = {'query.return_value': []}

        dummy_query_runner_for_manager = Mock(**dummy_arks)
        self.hash_sum_manager = HashSumManager(
            hash_sum_table="hashsum",
            target_table="target",
            query_runner=dummy_query_runner_for_manager
        )

    def test_incremental_build(self):
        config = self.config
        config.job.ingestion.fetching_strategy.fetching_strategy = "INCREMENTAL"
        config.job.ingestion.fetching_strategy.incremental_strategy = "COLUMN-DATE"

        contexts = ExecutionContextBuilder(config=config, query_runner=self.query_runner_mock).build()

        self.for_each_assert_is_instance(contexts, IncrementalExecutionContext)

    def test_none_build(self):
        config = self.config
        config.job.ingestion.fetching_strategy.fetching_strategy = None
        with self.assertRaises(RequireNotNoneException):
            ExecutionContextBuilder(config=config, query_runner=None).build()

    def test_wrong_build(self):
        config = self.config
        config.job.ingestion.fetching_strategy.fetching_strategy = "WRONG"
        with self.assertRaises(RuntimeError):
            ExecutionContextBuilder(config=config, query_runner=None).build()

    def for_each_assert_is_instance(self, iterable, cls):
        for item in iterable:
            self.assertIsInstance(item, cls)
