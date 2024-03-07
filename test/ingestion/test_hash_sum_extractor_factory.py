from configurations.configurator import Configurator
from hash_sum_utils.hash_sum_extractor_factory import HashSumExtractorFactory, \
    RestHashSumExtractor
from unittest.case import TestCase


# TODO refactor constants
class TestHashSumExtractorFactory(TestCase):
    def setUp(self):
        self.config = Configurator().get_configurator()

    def test_none_create(self):
        # Arrange
        config = self.config
        config.job.ingestion.data_manager.hash_sum_extractor_impl = "REST"
        hash_sum_extractor_factory = HashSumExtractorFactory(config)

        # Act
        common_hash_sum_extractor = hash_sum_extractor_factory.create()

        # Assert
        self.assertIsInstance(common_hash_sum_extractor, RestHashSumExtractor)

    def test_wrong_create(self):
        # Arrange
        config = self.config
        config.job.ingestion.data_manager.hash_sum_extractor_impl = "test"
        hash_sum_extractor_factory = HashSumExtractorFactory(config)

        # Assert
        self.assertRaises(Exception, lambda _: hash_sum_extractor_factory.create())

    def test_empty_create(self):
        # Arrange
        config = self.config
        config.job.ingestion.data_manager.hash_sum_extractor_impl = None
        hash_sum_extractor = HashSumExtractorFactory(config).create()

        # Assert
        self.assertIsInstance(hash_sum_extractor, RestHashSumExtractor)
