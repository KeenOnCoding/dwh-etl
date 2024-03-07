from typing import Mapping

import pytest
import chispa
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from ingestion.enrichers.abstract_enricher import AbstractEnricher
from ingestion.enrichers.enricher_chain import EnricherChain


@pytest.fixture
def enricher_chain():
    return EnricherChain()


@pytest.fixture
def spark():
    return SparkSession.builder \
        .getOrCreate()


@pytest.fixture
def df(spark):
    data = [(1, 'Alice')]
    return spark.createDataFrame(data, ['age', 'name'])


@pytest.fixture
def with_column_renamed_enricher_cls():
    class WithColumnRenamedEnricher(AbstractEnricher):
        def __init__(self, old, new):
            self.old = old
            self.new = new

        def enrich(self, df: DataFrame, context: Mapping) -> DataFrame:
            return df.withColumnRenamed(self.old, self.new)

    return WithColumnRenamedEnricher


def test_proper_order_of_enrichers(enricher_chain, df, with_column_renamed_enricher_cls):
    enricher_cap = with_column_renamed_enricher_cls('age', 'Age')
    enricher_uncap = with_column_renamed_enricher_cls('Age', 'age')

    enricher_chain.add_enricher(enricher_cap, 1)
    enricher_chain.add_enricher(enricher_uncap, 2)
    assert enricher_chain.enrich(df, {}).columns == df.columns


def test_when_df_is_empty(enricher_chain, spark):
    schema = StructType([])
    df = spark.createDataFrame([], schema=schema)
    enriched_df = enricher_chain.enrich(df, {})
    chispa.assert_df_equality(df, enriched_df)