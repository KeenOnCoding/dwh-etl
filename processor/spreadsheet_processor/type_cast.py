from abc import ABC, abstractmethod
from datetime import datetime, date
from dateutil import parser

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DateType, TimestampType, StringType

from commons.logging.simple_logger import logger


class TypeCastFactory:
    @staticmethod
    def create(field):
        if isinstance(field.dataType, DateType):
            type_cast = DateTypeCast(field=field)
        elif isinstance(field.dataType, TimestampType):
            type_cast = TimestampTypeCast(field=field)
        else:
            type_cast = NumericTypeCast(field=field)
        logger.info(f"Using '{type(type_cast).__name__}' for '{field.name}' field")
        return type_cast


class AbstractCast(ABC):
    @abstractmethod
    def cast(self, dataframe) -> DataFrame:
        raise NotImplementedError

    @staticmethod
    def data_formatting(udf, dataframe, field) -> DataFrame:
        return dataframe.withColumn(
            field.name,
            udf(field.name)
        )


class DateTypeCast(AbstractCast):
    def __init__(self, field) -> None:
        self.field = field

    def cast(self, dataframe) -> DataFrame:
        return super().data_formatting(
            udf=udf(ValueModification.cast_str_to_date, DateType()),
            dataframe=dataframe,
            field=self.field
        )


class TimestampTypeCast(AbstractCast):
    def __init__(self, field) -> None:
        self.field = field

    def cast(self, dataframe) -> DataFrame:
        return super().data_formatting(
            udf=udf(ValueModification.cast_str_to_timestamp, TimestampType()),
            dataframe=dataframe,
            field=self.field
        )


class NumericTypeCast(AbstractCast):
    def __init__(self, field) -> None:
        self.field = field

    def cast(self, dataframe) -> DataFrame:
        return super().data_formatting(
            udf=udf(ValueModification.change_separated_format, StringType()),
            dataframe=dataframe,
            field=self.field
        ).withColumn(
            self.field.name,
            col(self.field.name).cast(self.field.dataType)
        )


class ValueModification:
    @staticmethod
    def cast_str_to_date(str_date) -> date:
        return parser.parse(str_date).date()

    @staticmethod
    def cast_str_to_timestamp(str_date) -> datetime:
        return parser.parse(str_date)

    @staticmethod
    def change_separated_format(value) -> str:
        return value.replace(',', '.')
