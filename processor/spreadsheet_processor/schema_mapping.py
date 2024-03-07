from commons.logging.simple_logger import logger
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StringType

from type_cast import TypeCastFactory


class SchemaMapper:

    def _get_table_schema(self, table: DataFrame) -> StructType:
        return table.schema

    def _get_schema_attribute(self, table: DataFrame) -> list:
        return self._get_table_schema(table).names

    def _check_column_equals(self, current_table: DataFrame,
                             target_table: DataFrame) -> bool:
        logger.info("Checking column equals")
        return set(self._get_schema_attribute(current_table)) \
               == set(self._get_schema_attribute(target_table))

    def cast_schema(self, current_table: DataFrame,
                    target_table: DataFrame) -> DataFrame:
        if not self._check_column_equals(current_table, target_table):
            raise RuntimeError("Table attribute set does not match target table")
        logger.info("Columns equals")
        for field in self._get_table_schema(target_table).fields:
            if not isinstance(field.dataType, StringType):
                current_table = TypeCastFactory.create(field).cast(current_table)
        return current_table
