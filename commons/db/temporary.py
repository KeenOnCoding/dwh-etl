import re
import uuid
from typing import Union

from sqlalchemy.schema import CreateTable, CreateIndex, MetaData, Table

from commons.db.query_runner import QueryRunner
from commons.logging.simple_logger import logger


class NoSuchTemporaryTableError(Exception):
    pass


def generate_temp_table_name(table_name: str, with_uuid=True) -> str:
    schema, table_name = table_name.split('.')
    temporary_name = f'{schema}.tmp_{table_name}'
    if with_uuid:
        temporary_name = f'{temporary_name}_{str(uuid.uuid4()).replace("-", "_")}'
    logger.info(f"Generated temporary table name: {temporary_name}")
    return temporary_name


def generate_replacement_query(replaceable_table_name: str, replacement_table_name: str) -> str:
    schema_1, name_1 = replaceable_table_name.split('.')
    logger.info(f"Generated table name replacement query ")
    return f"""
        BEGIN TRAN
            EXEC sp_rename '{replaceable_table_name}', {name_1}_old;
            EXEC sp_rename '{replacement_table_name}', {name_1};
            DROP TABLE {replaceable_table_name}_old;
        COMMIT
    """


def drop_table_query(table_to_drop):
    logger.info(f"Using drop table query: '{table_to_drop}' ")
    return f"""
            IF OBJECT_ID('{table_to_drop}', 'U') IS NOT NULL 
            DROP TABLE {table_to_drop}; 
        """


def drop_table(query_runner, table_to_drop):
    query_runner.execute(drop_table_query(table_to_drop))
    logger.info(f"Table {table_to_drop} was dropped")


class TableInspector:
    def __init__(self, table_name, engine, query_runner, origin=None):
        self.engine = engine
        self.query_runner = query_runner
        self.table_name = table_name
        self.origin = origin
        self.destroyed = False

    def mirror(self, other_table_name, no_index=True) -> 'TableInspector':
        if other_table_name == self.table_name:
            raise RuntimeError('Mirroring itself may incur data loss')

        drop_query = self.get_drop_table_query(with_table_rename_to=other_table_name)
        create_query = self.get_create_table_query(with_table_rename_to=other_table_name)

        queries = [drop_query, create_query]

        if not no_index:
            queries.append(self.get_create_indexes_query(with_table_rename_to=other_table_name))

        for query in queries:
            self.query_runner.execute(query)

        return TableInspector(other_table_name, self.engine, self.query_runner, self)

    def get_origin(self) -> Union['TableInspector', None]:
        return self.origin

    def get_index_type(self, index) -> str:
        query = "SELECT type_desc FROM sys.indexes " \
                f"WHERE name = \'{index.name}\' " \
                f"AND object_id = OBJECT_ID(\'{self.table_name}\') " \
                "AND is_hypothetical = 0 AND is_primary_key = 0 " \
                "AND is_unique_constraint = 0 AND is_unique = 0"
        index_type = self.query_runner.query_single_result(query)
        return index_type

    def get_create_indexes_query(self, with_table_rename_to=None) -> str:
        queries = []
        for index in self.get_table_info().indexes:
            create_query = self.get_create_index_query(index)
            queries.append(create_query)

        query = "\n".join(queries)
        if with_table_rename_to:
            query = query.replace(self.table_name, with_table_rename_to)

        return query

    def get_create_index_query(self, index) -> str:
        index_type = self.get_index_type(index)
        from sqlalchemy.dialects import mssql
        import re

        # SQLAlchemy 1.4.36 produces bad query for creating index
        # it will be in form of CREATE INDEX index_name on table_name () INCLUDE (actual_column_list)
        # so I retrieve actual list of columns and substitute the whole section from empty parentheses
        # till the end of string
        create_index = str(CreateIndex(index).compile(dialect=mssql.dialect()))

        columns = re.search(r'\([\w, ]+?\)', create_index).group()
        create_index = re.sub(r'\(.+?\)', columns, create_index)

        return create_index.replace('INDEX', f'{index_type} INDEX')

    def get_table_info(self) -> Table:
        schema, name = self.table_name.split('.')
        meta = MetaData(bind=self.engine)
        return Table(name, meta, autoload_with=self.engine, schema=schema)

    def get_create_table_query(self, with_table_rename_to=None, compile_to_native=True) -> str:
        # compile with engine to properly handle VARCHAR(MAX), NVARCHAR(MAX)
        create_table = CreateTable(self.get_table_info())
        if compile_to_native:
            create_table = create_table.compile(self.engine)

        create_table_query = str(create_table).replace('"', '')

        query = re.sub(r'(CONSTRAINT\s\b\w+\b)', '', create_table_query)
        if with_table_rename_to:
            query = query.replace(self.table_name, with_table_rename_to)
        return query

    def get_drop_table_query(self, with_table_rename_to=None) -> str:
        table_name = with_table_rename_to or self.table_name

        return drop_table_query(table_name)

    def destroy(self) -> None:
        self.query_runner.execute(self.get_drop_table_query())
        self.destroyed = True


class TemporaryTableManager:
    def __init__(self, query_runner: QueryRunner):
        self.query_runner = query_runner
        self.temporary_to_inspector = dict()
        self.engine = query_runner.engine

    def register(self, origin: str) -> str:
        temporal_table_name = generate_temp_table_name(origin, with_uuid=True)
        if temporal_table_name in self.temporary_to_inspector:
            raise RuntimeError(f'Temporary table {temporal_table_name} already exists.')
        origin_inspector = TableInspector(origin, self.engine, self.query_runner)
        self.temporary_to_inspector[temporal_table_name] = origin_inspector.mirror(other_table_name=temporal_table_name,
                                                                                   no_index=True)
        return temporal_table_name

    def unregister(self, table_name, soft=False) -> TableInspector:
        if table_name not in self.temporary_to_inspector:
            raise NoSuchTemporaryTableError(f"Table with name {table_name} not found")

        if not soft:
            inspector = self.temporary_to_inspector[table_name]
            inspector.destroy()

        inspector = self.temporary_to_inspector[table_name]
        del self.temporary_to_inspector[table_name]

        return inspector

    def create_temporal(self, original_table_name: str) -> str:
        temporary_table_name = self.register(original_table_name)
        return temporary_table_name

    def delete_all(self) -> None:
        for table in self.temporary_to_inspector.copy():
            self.unregister(table, soft=False)
