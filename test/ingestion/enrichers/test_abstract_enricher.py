import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import lit
from ingestion.enrichers.abstract_enricher import AbstractEnricher


@pytest.fixture
def spark():
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def df(spark):
    schema = StructType([])
    return spark.createDataFrame([], schema=schema)


def test_should_fail_when_no_such_column(df):
    with pytest.raises(ValueError):
        AbstractEnricher.raise_on_column_absence(df, 'some-column')


def test_should_pass_with_existent_column(df: DataFrame):
    df = df.withColumn("some-column", lit(None))
    AbstractEnricher.raise_on_column_absence(df, 'some-column')
