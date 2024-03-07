from unittest import TestCase
from unittest.mock import Mock

from hash_sum_utils.hash_sum_manager import HashSumManager


# TODO refactor constants
class TestHashSumManager(TestCase):

    def setUp(self) -> None:
        self.query_runner_mock = Mock()

    def test_save(self):
        # Arrange
        util_table = 'check_sum_table'
        target_table = 'target_table'

        check_sum_manager = HashSumManager(hash_sum_table=util_table,
                                           target_table=target_table,
                                           query_runner=self.query_runner_mock)

        # Act
        check_sum_manager.save('value', 0, 'key')

        # Assert
        expected = "MERGE check_sum_table AS [target] " \
                   "USING (SELECT 0 batch_id, 'target_table' table_name, " \
                   "'key' request_parameters, 'value' hash_sum) AS [source] " \
                   "ON [target].table_name = [source].table_name " \
                   "AND [target].request_parameters = " \
                   "[source].request_parameters " \
                   "AND [target].hash_sum = [source].hash_sum " \
                   "WHEN MATCHED THEN " \
                   "UPDATE SET [target].batch_id = [source].batch_id " \
                   "WHEN NOT MATCHED THEN " \
                   "INSERT " \
                   "(batch_id, table_name, request_parameters, hash_sum)" \
                   " VALUES ([source].batch_id, [source].table_name, " \
                   "[source].request_parameters, [source].hash_sum);"

        self.query_runner_mock.execute.assert_called_with(expected)
