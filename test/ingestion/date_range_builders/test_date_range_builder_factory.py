from unittest import TestCase

import pytest

from commons.configurations.configurator import Configurator
from ingestion.date_range_builders.date_range_builder_factory \
    import IncrementalPayloadBuilderFactory
from ingestion.date_range_builders.granularity_aligner import GranularityAligner


# FIXME skip because of API Adapter
@pytest.mark.skip
class TestDateRangeBuilderFactory(TestCase):
    def setUp(self) -> None:
        self.config = Configurator().get_configurator()

    def test_column_date_build(self):
        # Arrange
        self.config.job.ingestion.fetching_strategy.incremental_strategy = 'COLUMN-DATE'

        # Act
        actual = IncrementalPayloadBuilderFactory(self.config, None).create()

        # Assert
        self.assertIsInstance(actual, GranularityAligner)

    def test_column_date_GM_build(self):
        # Arrange
        config = self.config
        config.job.ingestion.fetching_strategy.incremental_strategy = 'CUSTOM-GM'

        # Act
        actual = IncrementalPayloadBuilderFactory(config, None).create()

        # Assert
        self.assertIsInstance(actual,GranularityAligner)

    def test_none_build(self):
        # Arrange
        config = self.config
        config.job.ingestion.fetching_strategy.incremental_strategy = None

        # Act
        with self.assertRaises(Exception) as context:
            IncrementalPayloadBuilderFactory(config, None).create()

        # Assert
        self.assertTrue('INCREMENTAL_STRATEGY is not set' in str(context.exception))

    def test_wrong_build(self):
        # Arrange
        config = self.config
        config.job.ingestion.fetching_strategy.incremental_strategy = 'WRONG'

        # Act
        with self.assertRaises(Exception) as context:
            IncrementalPayloadBuilderFactory(config, None).create()

        # Assert
        self.assertTrue('INCREMENTAL_STRATEGY is incorrect' in str(context.exception))
