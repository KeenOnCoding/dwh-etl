import uuid

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf, col

from commons.configurations.configurator import Configurator
from ingestion.enrichers.surrogate_key_enricher import SurrogateKeyEnricher


@pytest.fixture
def spark():
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def alice_df(spark):
    return spark.createDataFrame([('19', 'Alice')], ['age', 'name'])


def test_if_generated_column_is_uuid(alice_df):
    cfg = Configurator().get_configurator()
    key = 'key'
    cfg.job.ingestion.enrichers.surrogate_key_columns = key

    enricher = SurrogateKeyEnricher(cfg)
    enriched_df = enricher.enrich(alice_df, {})

    def is_uuid_valid(value):
        try:
            uuid.UUID(value)
            return True
        except ValueError:
            return False

    is_uuid_valid_udf = udf(is_uuid_valid, BooleanType())

    non_uuid_count = enriched_df.select(is_uuid_valid_udf(col(key)).alias('is_uuid')) \
        .filter(col('is_uuid') == False).collect()

    assert len(non_uuid_count) == 0
