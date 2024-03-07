import pytest

from commons.spark.spark_db_sink import AbstractDatabaseSink
from ingestion.enrichers.abstract_enricher import AbstractEnricher


@pytest.fixture
def data():
    return 'data'


@pytest.fixture
def count_data():
    return 42


@pytest.fixture
def countable(count_data):
    class Countable:
        def count(self):
            return count_data

    return Countable()


@pytest.fixture
def data_source(countable):
    class Extractable:
        def extract_data(self, context):
            return countable

    return Extractable()


@pytest.fixture
def enricher(countable):
    class Enricher(AbstractEnricher):
        def enrich(self, df, context):
            return countable

    return Enricher()


@pytest.fixture
def sink():
    class Sink(AbstractDatabaseSink):
        def __init__(self):
            self.state = None
            self.table_name = None

        def save(self, data, table_name):
            self.state = data
            self.table_name = table_name

    return Sink()
