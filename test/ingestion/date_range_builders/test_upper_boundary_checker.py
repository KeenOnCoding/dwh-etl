from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock

import constants
from commons.configurations.configurator import Configurator

from date_range_builders.upper_boundary_checker import UpperBoundaryChecker
from ingestion.constants import DEFAULT_DATETIME_PATTERN


class TestUpperBoundaryChecker(TestCase):
    def setUp(self) -> None:
        arks = {'build.return_value': (
            datetime.strptime('2020-10-18 00:00:00', DEFAULT_DATETIME_PATTERN),
            datetime.strptime('2020-10-19 00:00:00', DEFAULT_DATETIME_PATTERN)
        )
        }
        self.date_range_builder_mock = Mock(**arks)

        self.config = Configurator().get_configurator()

    def test_upper_bound_not_set_build(self):
        config = self.config

        actual = UpperBoundaryChecker(config, self.date_range_builder_mock).build()

        expected = (
            datetime(2020, 10, 18, 00, 00, 00),
            datetime(2020, 10, 19, 00, 00, 00)
        )

        self.assertEqual(actual, expected)

    def test_upper_bound_build(self):
        config = self.config
        config.job.ingestion.fetching_strategy.date_range_upper_bound = 12
        config.job.ingestion.fetching_strategy.granularity = constants.MONTHS

        actual = UpperBoundaryChecker(config, self.date_range_builder_mock).build()

        expected = (
            datetime(2020, 10, 18, 00, 00, 00),
            datetime(2020, 10, 19, 00, 00, 00)
        )

        self.assertEqual(actual, expected)
