from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import DecimalType

from commons.logging.simple_logger import logger
from ingestion.enrichers.abstract_enricher import AbstractEnricher


class BaseTypeCaster:

    def __init__(self, strategy_name, cast_type):
        self.strategy_name = strategy_name
        self.cast_type = cast_type

    def get_strategy_name(self) -> str:
        return self.strategy_name

    def cast(self, df: DataFrame, column_name) -> DataFrame:
        mutated_df = df.withColumn(column_name, regexp_replace(column_name, ',', '.')) \
            .withColumn(column_name, col(column_name).cast(self.cast_type))

        nulls = mutated_df.filter(col(column_name).isNull()).count()
        if nulls > 0:
            raise RuntimeError(f'Some values were truncated when casting to: {self.cast_type}')

        return mutated_df


class DecimalCaster(BaseTypeCaster):
    def __init__(self, precision, scale):
        BaseTypeCaster.__init__(self, "decimal", DecimalType(precision, scale))


class TypeCasterFacade:
    DEFAULT_DECIMAL_PRECISION = 16
    DEFAULT_DECIMAL_SCALE = 2

    def __init__(self, config):
        self.config = config

    def get_caster(self, cast_expr) -> BaseTypeCaster:
        try:
            cast_type, *parameters = cast_expr.split("-")
            precision, *scale = [int(x) for x in parameters]
        except ValueError:
            cast_type = cast_expr
            precision = TypeCasterFacade.DEFAULT_DECIMAL_PRECISION
            scale = [TypeCasterFacade.DEFAULT_DECIMAL_SCALE]

        if cast_type == 'decimal':
            return DecimalCaster(precision,
                                 scale[0] if scale else TypeCasterFacade.DEFAULT_DECIMAL_SCALE)

        else:
            raise RuntimeError(f'No such caster found: {cast_type}')


class ColumnTypeCasterEnricher(AbstractEnricher):
    def __init__(self, casters_facade: TypeCasterFacade,
                 config):
        self.casters_facade = casters_facade
        self.cast_types = config.job.ingestion.enrichers.explicit_cast

    def enrich(self, df: DataFrame, context) -> DataFrame:
        if self.cast_types is not None:
            logger.info(f"Using ColumnTypeCasterEnricher {self.cast_types}")
            for i in self.cast_types:
                try:
                    column, cast_expr = i["column_name"], i["cast_type"]
                except TypeError:
                    raise TypeError(f'Cannot unpair column:cast_expr: {i}.')
                self.raise_on_column_absence(df, column)
                df = self.casters_facade.get_caster(cast_expr).cast(df, column)
        return df
