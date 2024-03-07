from datetime import datetime
from functools import partial

import pytest
import chispa
from pyspark.sql import SparkSession

from commons.configurations.configurator import Configurator
from ingestion.enrichers.iso_date_aligner import ToDatetimeEnricher


@pytest.fixture
def spark():
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def alice_df(spark):
    return spark.createDataFrame([(19, 'Alice')], ['age', 'name'])


@pytest.fixture
def partial_enricher():

    config = Configurator().get_configurator()

    def conf_init(column, dformat):
        config.job.ingestion.enrichers.date_column_to_datetime = column
        config.job.ingestion.enrichers.date_column_origin_format = dformat
        return config

    return partial(lambda column=None, dformat=None: ToDatetimeEnricher(
        conf_init(column, dformat)
    ))


def test_raise_when_no_such_column(alice_df, partial_enricher):
    with pytest.raises(ValueError):
        partial_enricher('my_column', 'anyformat').enrich(alice_df, {})


@pytest.mark.parametrize('column_to_format', [(None, 'format'), ('column', None)])
def test_raise_when_one_of_enricher_external_params_is_absent(alice_df, partial_enricher, column_to_format):
    column, format_ = column_to_format
    with pytest.raises(RuntimeError):
        partial_enricher(column, format_).enrich(alice_df, {})


def test_skip_if_both_external_params_are_none(alice_df, partial_enricher):
    expected_df = alice_df
    actual_df = partial_enricher().enrich(alice_df, {})
    chispa.assert_df_equality(expected_df, actual_df)


@pytest.mark.parametrize('date_origin_format', ['%m/%d/%Y, %H:%M:%S', '%d.%m.%Y %H:%M:%S'])
@pytest.mark.parametrize('expected_datetime', [
    datetime(year=2022, month=11, day=23),
    datetime(year=2034, month=5, day=1, hour=23)
])
def test_date(date_origin_format, expected_datetime: datetime, partial_enricher, spark):
    string_date_with_origin_format = expected_datetime.strftime(date_origin_format)

    column_name = 'value'
    enricher = partial_enricher(column_name, date_origin_format)

    # load data from json string to prevent automatic type casting by spark
    value_entity = {column_name: string_date_with_origin_format}
    import json
    json_str = json.dumps(value_entity)
    date_rdd = spark.sparkContext.parallelize([json_str])
    df = spark.read.json(date_rdd, primitivesAsString=True)

    dates = enricher.enrich(df, {}).select(column_name).collect()

    for date in dates:
        actual_datetime = date[0]
        assert expected_datetime == actual_datetime
