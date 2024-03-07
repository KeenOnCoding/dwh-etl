from typing import List


class Table:

    def __init__(self, name: str, partition_column: str):
        self.name = name
        self.partition_column = partition_column


class QueryDef:

    def __init__(self, sql: str, description: str,
                 staging_table: str = None, partition_column: str = None,
                 staging_table_alias: str = None, tables_to_cleanup: str = None):
        self.sql = sql
        self.description = description
        self.staging_table = staging_table
        self.staging_table_alias = staging_table_alias
        self.partition_column = partition_column
        self.tables_to_cleanup = tables_to_cleanup

    def __repr__(self):
        return (f"Description: {self.description}\n"
                f"Partition column: {self.partition_column}\n"
                f"Staging table: {self.staging_table}\n"
                f"Staging table alias: {self.staging_table_alias} \n"
                f"SQL:\n {self.sql}")


class QueryPlanDef:

    def __init__(self, tables: List[Table], queries: List[QueryDef],
                 variables: dict):
        self.tables = tables
        self.queries = queries
        self.variables = variables
