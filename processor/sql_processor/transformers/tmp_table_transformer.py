import copy
import re

from commons.db.temporary import TableInspector
from commons.db.temporary import generate_temp_table_name, generate_replacement_query
from commons.logging.simple_logger import logger
from processor.sql_processor.queries.query_plan_definitions import QueryPlanDef, QueryDef
from processor.sql_processor.transformers.query_transformer import QueryTransformer


class TmpTableTransformer(QueryTransformer):

    def __init__(self, engine, query_runner, config):
        self.engine = engine
        self.query_runner = query_runner
        self.config = config

    def transform(self, query_plan_def: QueryPlanDef) -> QueryPlanDef:
        plan = copy.deepcopy(query_plan_def)

        tables_to_overwrite = plan.variables['TABLES_TO_REWRITE'] if 'TABLES_TO_REWRITE' in plan.variables else []
        logger.info(f"Tables to overwrite: {tables_to_overwrite}")

        for table_name in tables_to_overwrite:

            inspector = TableInspector(table_name, self.engine, self.query_runner)

            temporary_table_name = generate_temp_table_name(table_name, with_uuid=False)
            logger.info(f"Generated temporary table name: {temporary_table_name} for table {table_name}")

            create_indexes_query = inspector.get_create_indexes_query()
            # query might be empty because of absence of any index on the table
            if create_indexes_query:
                plan.queries.insert(0, QueryDef(create_indexes_query, 'recreate indexes'))

            # disable compiling to prevent PK clashes
            # when query is not compiled, it produces the primary key constraint in form of
            # `PRIMARY KEY (column)`. But when compiled, it produces something like that
            # `CONSTRAINT [PK__tmp_dim___3213E83F181A6256] PRIMARY KEY (colum)`
            create_table_query = inspector.get_create_table_query(compile_to_native=False)
            plan.queries.insert(0, QueryDef(create_table_query, 'create temporary table'))

            drop_table_query = inspector.get_drop_table_query()
            plan.queries.insert(0, QueryDef(drop_table_query, 'drop temporary table'))

            # replace table name occurrence in every query definition for temporary one
            for query_def in plan.queries:
                query_def.sql = re.sub(fr'{table_name}(?!\w)', temporary_table_name, query_def.sql)

            replacement_query = generate_replacement_query(table_name, temporary_table_name)
            logger.info(f"Replaced '{table_name}' table name occurrence in every query definition"
                        f" for temporary '{temporary_table_name}'")
            plan.queries.append(QueryDef(replacement_query, 'replace current table with temporary one'))

        return plan
