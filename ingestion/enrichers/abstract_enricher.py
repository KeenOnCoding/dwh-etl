from abc import ABC
from abc import abstractmethod
from collections.abc import Mapping
from pyspark.sql import DataFrame


class AbstractEnricher(ABC):
    """
    Base class for all enrichers.
    """

    @abstractmethod
    def enrich(self, df: DataFrame, context: Mapping) -> DataFrame:
        raise NotImplementedError

    @staticmethod
    def raise_on_column_absence(df: DataFrame, column: str):
        if column not in df.columns:
            raise ValueError(f'No such column in dataframe: {column}')
