from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock

from commons.configurations.configurator import Configurator
from ingestion import constants
from ingestion.date_range_builders.column_date_range_builder import ColumnIncrementalPayloadBuilder


class TestColumnDateRangeBuilder(TestCase):
    def setUp(self) -> None:
        arks = {'query_single_result.return_value':
                    datetime.strptime('2020-10-18 00:00:00',
                                      '%Y-%m-%d %H:%M:%S')}
        empty_arks = {'query_single_result.return_value': None}

        self.query_runner_mock = Mock(**arks)
        self.empty_query_runner_mock = Mock(**empty_arks)

        self.config = Configurator().get_configurator()

    def test_none_build(self):
        config = self.config
        date_range_builder = ColumnIncrementalPayloadBuilder(config, self.query_runner_mock)

        actual = date_range_builder.build()

        expected = (
            datetime(2020, 10, 18, 00, 00, 00),
            datetime(2020, 10, 20, 00, 00, 00, 00)
        )
        self.assertEqual(actual, expected)

    def test_batch_window_build(self):
        config = self.config
        config.job.ingestion.fetching_strategy.batch_window = 71
        config.job.ingestion.fetching_strategy.granularity = constants.HOURS

        date_range_builder = ColumnIncrementalPayloadBuilder(config, self.query_runner_mock)

        # Act
        actual = date_range_builder.build()

        # Assert
        expected = (
            datetime(2020, 10, 18, 00, 00, 00),
            datetime(2020, 10, 20, 23, 00, 00)
        )

        self.assertEqual(actual, expected)

    def test_date_from_time_offset_build(self):
        config = self.config
        config.job.ingestion.fetching_strategy.batch_window = 71
        config.job.ingestion.fetching_strategy.date_from_time_offset = 24
        config.job.ingestion.fetching_strategy.granularity = constants.HOURS

        date_range_builder = ColumnIncrementalPayloadBuilder(config,
                                                             self.query_runner_mock)
        actual = date_range_builder.build()

        expected = (
            datetime(2020, 10, 19, 00, 00, 00),
            datetime(2020, 10, 21, 23, 00, 00)
        )

        self.assertEqual(actual, expected)

    def test_empty_target_table_build_with_default_value(self):
        config = self.config
        config.job.ingestion.fetching_strategy.batch_window = 71
        config.job.ingestion.fetching_strategy.granularity = constants.HOURS
        config.job.ingestion.fetching_strategy.default_partition_value = '2020-10-19 00:00:00'

        date_range_builder = ColumnIncrementalPayloadBuilder(config, self.empty_query_runner_mock)

        actual = date_range_builder.build()

        expected = (
            datetime(2020, 10, 19, 00, 00, 00),
            datetime(2020, 10, 21, 23, 00, 00)
        )

        self.assertEqual(actual, expected)

    def test_empty_target_table_build_with_wrong_default_value_format(self):
        config = self.config
        config.job.ingestion.fetching_strategy.batch_window = 71
        config.job.ingestion.fetching_strategy.granularity = constants.HOURS
        config.job.ingestion.fetching_strategy.default_partition_value = '2020-10-18'

        date_range_builder = ColumnIncrementalPayloadBuilder(config, self.empty_query_runner_mock)

        self.assertRaises(ValueError, date_range_builder.build)

    def test_empty_target_table_build_without_default_value(self):
        config = self.config
        config.job.ingestion.fetching_strategy.batch_window = 71
        config.job.ingestion.fetching_strategy.granularity = constants.HOURS

        date_range_builder = ColumnIncrementalPayloadBuilder(config, self.empty_query_runner_mock)

        self.assertRaises(ValueError, date_range_builder.build)
