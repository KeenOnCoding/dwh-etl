from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from commons.configurations.configurator import Configurator
from ingestion.enrichers.typecasters import ColumnTypeCasterEnricher, TypeCasterFacade

@pytest.fixture
def config():
    return Configurator().get_configurator()

@pytest.fixture
def spark():
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def alice_df(spark):
    return spark.createDataFrame([('19', 'Alice')], ['age', 'name'])


@pytest.fixture
def wrong_config(config):
    config.job.ingestion.enrichers.explicit_cast = 'no-such-explicit-cast'
    return config


@pytest.fixture
def wrong_caster_config(config):
    explicit_cast = [
        {
            "column_name": "age",
            "cast_type": "caster"
        }
    ]
    config.job.ingestion.enrichers.explicit_cast = explicit_cast
    return config


def test_no_such_column(alice_df, config):
    explicit_cast = [
        {
            "column_name": "column",
            "cast_type": "decimal"
        }
    ]
    config.job.ingestion.enrichers.explicit_cast = explicit_cast
    column_caster = make_column_caster(config)
    with pytest.raises(ValueError):
        column_caster.enrich(alice_df, {})


def make_column_caster(config):
    return ColumnTypeCasterEnricher(TypeCasterFacade(config), config)


def test_wrong_explicit_cast_format(wrong_config, alice_df):
    column_caster = make_column_caster(wrong_config)
    with pytest.raises(TypeError):
        column_caster.enrich(alice_df, {})


def test_wrong_caster(wrong_caster_config, alice_df):
    column_caster = make_column_caster(wrong_caster_config)
    with pytest.raises(RuntimeError):
        column_caster.enrich(alice_df, {})


@pytest.mark.parametrize('value', ['19', '23.001', '.00001', '20,23'])
@pytest.mark.parametrize('scale_prec', [
    (),
    ('8', '5'),
    ('10',),
])
def test_decimal_caster(value, scale_prec, spark, config):
    explicit_cast = [
        {
            "column_name": "value",
            "cast_type": f"decimal{'-' if scale_prec else ''}{'-'.join(scale_prec)}"
        }
    ]
    config.job.ingestion.enrichers.explicit_cast = explicit_cast

    df = spark.createDataFrame([(value,)], ['value', ])
    column_caster = make_column_caster(config)
    df = column_caster.enrich(df, {})
    actual_value = df.select('value').collect()[0][0]

    prec = scale_prec[1] if len(scale_prec) == 2 else TypeCasterFacade.DEFAULT_DECIMAL_SCALE
    # rescale expected to compare against actual value
    expected_value = Decimal(f"{float(value.replace(',', '.')):.{prec}f}")

    assert actual_value == expected_value


@pytest.mark.parametrize('precision_scale', [
    (5, 3),
    (2, 1),
    (4, 3)
])
def test_raise_when_scale_less_than_actual_value(spark, precision_scale, config):
    prec, scale = precision_scale
    explicit_cast = [
        {
            "column_name": "value",
            "cast_type": f"decimal-{prec}-{scale}"
        }
    ]
    config.job.ingestion.enrichers.explicit_cast = explicit_cast
    really_big_value = '12345678910111200.23'
    df = spark.createDataFrame([(really_big_value,)], ['value'])
    column_caster = make_column_caster(config)

    with pytest.raises(RuntimeError):
        column_caster.enrich(df, {})
