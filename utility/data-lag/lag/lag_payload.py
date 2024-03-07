from enum import Enum


class LagPayload:
    __slots__ = ('schema', 'table_name', 'check_strategy', 'threshold_value',
                 'partition_column')

    def __init__(self,
                 schema,
                 table_name,
                 lag_check_strategy,
                 threshold,
                 partition_column):
        self.schema = schema
        self.table_name = table_name
        self.check_strategy = lag_check_strategy
        self.threshold_value = threshold
        self.partition_column = partition_column


class PayloadColumn(str, Enum):
    SCHEMA = "schema"
    TABLE_NAME = "table_name"
    PARTITION_COLUMN = "partition_column"
    CHECK_STRATEGY = "check_strategy"
    THRESHOLD_VALUE = "threshold_value"
