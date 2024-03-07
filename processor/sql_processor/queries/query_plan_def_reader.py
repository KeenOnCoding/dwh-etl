import os
import warnings
import yaml

from pathlib import Path
from sqlalchemy import String
from commons.logging.simple_logger import logger
from commons.utils.file_utils import find_files_with_pattern
from processor.sql_processor.queries.query_plan_definitions import QueryPlanDef, Table, QueryDef


class QueryPlanDefReader:

    def __init__(self, config, prod=True):
        self.config = config
        self.plan_path = os.path.join(Path(__file__).parents[3],
                                      Path(f'configs/sql-processor-configs/{"production" if prod else "test"}/'))

    def read(self, plan_id: String) -> QueryPlanDef:
        """Reading  file id.yaml from resources/configs"""

        files = find_files_with_pattern(plan_id + '.yaml', self.plan_path)
        if len(files) > 1:
            raise RuntimeError("Found multiple files that match "
                               "PLAN_ID pattern.\n"
                               f"PLAN_ID: {plan_id}.\n"
                               f"Found files: {files}\n"
                               f"Root search directory: "
                               f"{self.plan_path}")

        try:
            plan_id_file_path = files[0]
        except IndexError:
            raise RuntimeError(f"No plan id with name [{plan_id}] "
                               f"found from root directory "
                               f"[{self.plan_path}].\n"
                               f"Consider that QueryPlanDefReader is "
                               f"searching files from the 3rd directory up "
                               f"from itself.")

        logger.info(f"Reading query from {plan_id_file_path} file")
        with open(plan_id_file_path, 'r') as stream:
            data_loaded = yaml.safe_load(stream)
            query_plan = data_loaded['query-plan']
            state = query_plan.get('state') or 'active'

            if state == 'disabled':
                reason = query_plan.get('reason') or 'No reason provided'
                raise RuntimeError(f'Terminating processing because script is marked as disabled with reason: {reason}')

            try:
                partition_column = query_plan['tables']['partition_column']
            except KeyError:
                warnings.warn('No partition column found.')

                partition_column = None

            tables = [Table(name=table_name, partition_column=partition_column) for table_name in
                      query_plan['tables']['table_names']]

            queries = []
            for query in query_plan['queries']:
                sql = query['sql']
                description = query['description']
                staging_table_with_alias = query.get('staging-table', None)

                if staging_table_with_alias:
                    staging_table, *alias = staging_table_with_alias.split('~')
                    alias = alias[0] if alias else None
                else:
                    staging_table = None
                    alias = None

                tables_to_cleanup = query.get('auto-cleanup', [])

                partition_column = query.get('partition-column', None)

                queries.append(QueryDef(sql, description, staging_table, partition_column, alias, tables_to_cleanup))

            try:
                variables = query_plan['variables']
            except KeyError:
                variables = {}
            return QueryPlanDef(tables, queries, variables)
