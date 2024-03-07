from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock

from commons.configurations.configurator import Configurator
from ingestion.query_params_builders.from_to_query_params_builder \
    import FromToQueryParamsBuilder


class TestFromToQueryParamsBuilder(TestCase):

    def setUp(self) -> None:
        arks = {
            'get_from_to_dates.return_value': (datetime(2020, 10, 18, 00, 00, 00), datetime(2020, 10, 20, 00, 00, 00))
        }
        self.batch_context = Mock(**arks)
        self.config = Configurator().get_configurator()

    def test_no_date_format_build(self):
        config = self.config
        config.job.ingestion.general.from_to_params_names = ["modifiedFrom", "modifiedTo"]

        actual = FromToQueryParamsBuilder(config).build(self.batch_context)
        expected = 'modifiedFrom=2020-10-18T00:00:00&modifiedTo=2020-10-20T00:00:00'

        self.assertEqual(expected, actual)

    def test_date_format_build(self):
        config = self.config
        config.job.ingestion.general.from_to_params_names = ["modifiedFrom", "modifiedTo"]
        config.job.ingestion.general.date_format = '%Y-%m-%d'

        actual = FromToQueryParamsBuilder(config).build(self.batch_context)
        expected = 'modifiedFrom=2020-10-18&modifiedTo=2020-10-20'

        self.assertEqual(expected, actual)
