from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock

from commons.configurations.configurator import Configurator
from ingestion import constants
from ingestion.date_range_builders.granularity_aligner import GranularityAligner


class TestGranularityAligner(TestCase):

    def setUp(self) -> None:
        arks = {'build.return_value': (
            datetime.strptime('2020-10-18 23:30:20',
                              '%Y-%m-%d %H:%M:%S'),
            datetime.strptime('2020-10-19 17:45:26',
                              '%Y-%m-%d %H:%M:%S')
        )}
        self.date_range_builder_mock = Mock(**arks)
        self.config = Configurator().get_configurator()

    def test_granularity_not_set_build(self):
        config = self.config
        config.job.ingestion.fetching_strategy.align_dates = True

        actual = GranularityAligner(config, self.date_range_builder_mock).build()

        expected = (
            datetime(2020, 10, 18, 23, 00, 00),
            datetime(2020, 10, 19, 17, 00, 00)
        )

        self.assertEqual(expected, actual)

    def test_granularity_align_dates_not_set_build(self):
        config = self.config

        actual = GranularityAligner(config, self.date_range_builder_mock).build()

        expected = (
            datetime(2020, 10, 18, 23, 30, 20),
            datetime(2020, 10, 19, 17, 45, 26)
        )

        self.assertEqual(expected, actual)

    def test_month_granularity_build(self):
        config = self.config
        config.job.ingestion.fetching_strategy.granularity = constants.MONTHS
        config.job.ingestion.fetching_strategy.align_dates = True

        actual = GranularityAligner(config, self.date_range_builder_mock).build()

        expected = (
            datetime(2020, 10, 1, 00, 00, 00),
            datetime(2020, 10, 1, 00, 00, 00)
        )

        self.assertEqual(actual, expected)

    def test_day_granularity_build(self):
        config = self.config
        config.job.ingestion.fetching_strategy.granularity = constants.DAYS
        config.job.ingestion.fetching_strategy.align_dates = True

        actual = GranularityAligner(config, self.date_range_builder_mock).build()

        expected = (
            datetime(2020, 10, 18, 00, 00, 00),
            datetime(2020, 10, 19, 00, 00, 00)
        )

        self.assertEqual(expected, actual)

    def test_hour_granularity_build(self):
        config = self.config
        config.job.ingestion.fetching_strategy.granularity = constants.HOURS
        config.job.ingestion.fetching_strategy.align_dates = True

        actual = GranularityAligner(config, self.date_range_builder_mock).build()

        expected = (
            datetime(2020, 10, 18, 23, 00, 00),
            datetime(2020, 10, 19, 17, 00, 00)
        )

        self.assertEqual(expected, actual)

    def test_wrong_granularity_build(self):
        config = self.config
        config.job.ingestion.fetching_strategy.granularity = 'WRONG_GRANNO'
        config.job.ingestion.fetching_strategy.align_dates = True

        with self.assertRaises(ValueError):
            GranularityAligner(config, self.date_range_builder_mock).build()
