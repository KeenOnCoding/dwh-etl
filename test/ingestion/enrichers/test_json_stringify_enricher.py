import chispa
from functools import partial
import pytest
import json
from pyspark.sql import SparkSession
from commons.configurations.configurator import Configurator

from ingestion.enrichers.json_stringify_enricher import JsonStringifyEnricher


@pytest.fixture
def spark():
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def alice_df(spark):
    return spark.createDataFrame([(1, 'Alice')], ['age', 'name'])


@pytest.fixture
def partial_enricher():

    config = Configurator().get_configurator()

    def conf_init(column):
        config.job.ingestion.enrichers.stringify_json_columns = column
        return config

    return partial(lambda column=None: JsonStringifyEnricher(
        conf_init(column)
    ))


def test_raise_when_no_such_column(alice_df, partial_enricher):
    with pytest.raises(ValueError):
        partial_enricher(column='my_column').enrich(alice_df, {})


def test_return_unchanged_if_external_unset(alice_df, partial_enricher):
    expected_df = alice_df
    actual_df = partial_enricher().enrich(alice_df, {})

    chispa.assert_df_equality(expected_df, actual_df)


@pytest.fixture
def alice_nested_dict():
    return {
        'name': 'Alice',
        'age': 22,
        'framework': ['Spark', 'Spring'],
        'hobbies': ['swimming', 'reading'],
        'object': {
            'name': 'Some-Object',
            'age': 42
        }
    }


@pytest.fixture
def alice_df_nested(spark, alice_nested_dict):
    json_dict = json.dumps(alice_nested_dict)
    return spark.read.json(spark.sparkContext.parallelize([json_dict]))


@pytest.mark.parametrize('column', ['framework', 'hobbies', 'object'])
def test_stringify_nested(alice_df_nested, partial_enricher, column, alice_nested_dict):
    columns = [column]
    enricher = partial_enricher(column=columns)
    expected_object = alice_nested_dict[column]

    enriched_df = enricher.enrich(alice_df_nested, {})
    actual_stringified_column = enriched_df.select(column).collect()[0][0]
    actual_object = json.loads(actual_stringified_column)

    assert actual_object == expected_object
