from configurations.configurator import Configurator
from hash_sum_utils.rest_hash_sum_extractor import RestHashSumExtractor
from unittest.case import TestCase


# TODO refactor constants
class TestCommonHashSumExtractor(TestCase):
    def setUp(self) -> None:
        self.config = Configurator().get_configurator()
        self.config.job.ingestion.data_manager.hash_sum_response_key = 'key'

    def test_extract(self):
        response = '{"key": "value"}'

        # Arrange
        common_extractor = RestHashSumExtractor(self.config)

        # Act
        actual = common_extractor.extract(response)

        # Assert
        expected = 'value'
        self.assertEqual(expected, actual)

    def test_wrong_key_extract(self):
        # Arrange
        common_extractor = RestHashSumExtractor(self.config)

        # Assert
        self.assertRaises(Exception, lambda _: common_extractor.extract({}))
