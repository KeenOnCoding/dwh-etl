from datetime import datetime, date
from unittest import TestCase
from unittest.mock import Mock

import pytest
from dateutil.relativedelta import relativedelta

import constants
from commons.configurations.configurator import Configurator
from ingestion.date_range_builders.gm_date_range_builder import GmIncrementalPayloadBuilder


class TestGmDateRangeBuilder(TestCase):
    def setUp(self) -> None:
        self.config = Configurator().get_configurator()
        today = datetime.fromordinal(date.today().toordinal())
        query_single_result_return_value = 'query_single_result.return_value'

        normal_arks = {
            query_single_result_return_value: self.__month_delta(today, -1)
        }
        self.normal_query_runner_mock = Mock(**normal_arks)

        data_lag_arks = {
            query_single_result_return_value: datetime(2020, 1, 1)
        }
        self.lag_query_runner_mock = Mock(**data_lag_arks)

        empty_arks = {query_single_result_return_value: None}
        self.empty_mock = Mock(**empty_arks)

    def test_none_build(self):
        config = self.config
        date_range_builder = GmIncrementalPayloadBuilder(config, self.normal_query_runner_mock)

        actual = date_range_builder.build()

        today = datetime.fromordinal(date.today().toordinal())
        expected = (
            self.__month_delta(today, -1),
            self.__month_delta(today, 12)
        )

        self.assertEqual(actual, expected)

    @pytest.mark.skip
    def test_batch_window_build(self):
        config = self.config
        config.job.ingestion.fetching_strategy.batch_window = 5
        config.job.ingestion.fetching_strategy.granularity = constants.MONTHS

        date_range_builder = GmIncrementalPayloadBuilder(config, self.normal_query_runner_mock)

        actual = date_range_builder.build()

        today = datetime.fromordinal(date.today().toordinal())

        expected = (
            self.__month_delta(today, -1),
            self.__month_delta(today, 5)
        )

        self.assertEqual(actual, expected)

    def test_date_from_time_offset_build(self):
        config = self.config
        config.job.ingestion.fetching_strategy.date_from_time_offset = -38
        config.job.ingestion.fetching_strategy.granularity = constants.MONTHS

        date_range_builder = GmIncrementalPayloadBuilder(config, self.normal_query_runner_mock)

        actual = date_range_builder.build()

        today = datetime.fromordinal(date.today().toordinal())
        expected = (
            self.__month_delta(today, -38),
            self.__month_delta(today, 12)
        )

        self.assertEqual(expected, actual)

    def test_empty_table_build(self):
        config = self.config
        config.job.ingestion.fetching_strategy.granularity = constants.MONTHS

        date_range_builder = GmIncrementalPayloadBuilder(config, self.empty_mock)

        actual = date_range_builder.build()

        today = datetime.fromordinal(date.today().toordinal())

        expected = (
            self.__month_delta(today, -1),
            self.__month_delta(today, 12)
        )

        self.assertEqual(actual, expected)

    def __month_delta(self, date, delta):
        return date + relativedelta(months=delta)
