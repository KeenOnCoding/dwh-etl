from unittest import TestCase

from commons.configurations.configurator import Configurator
from queries.query_plan_def_reader import QueryPlanDefReader
from queries.query_plan_definitions import QueryPlanDef, QueryDef, Table


class TestQueryPlanDefReader(TestCase):

    def setUp(self) -> None:
        self.config = Configurator().get_configurator()
        self.config.job.processing.sql_processor.dynamic_variable = 'max_batch_window:241920'
        self.query_plan_def_reader = QueryPlanDefReader(self.config, prod=False)

    def test_read(self):
        query_plan_def = self.query_plan_def_reader.read('test_table_latest')
        
        actual_tables = list(map(lambda i: (i.name, i.partition_column), query_plan_def.tables))
        actual_queries = list(map(lambda i: (i.sql, i.description), query_plan_def.queries))
        actual_variables = query_plan_def.variables

        exp_tmp_tables = {'TABLES_TO_REWRITE': ['dbo.test_table_latest']}
        exp_tables = [('test_table_staging', 'version')]
        exp_queries = [(
            "IF OBJECT_ID('dbo.tmp_test_table_latest', 'U') IS NOT NULL\nDROP TABLE dbo.tmp_test_table_latest;\n",
            'drop table if it exists'), (
            "CREATE TABLE [sigma-dwh].dbo.tmp_test_table_latest (\n  id CHAR(36) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,\n  name NVARCHAR(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,\n  PRIMARY KEY (id));\n",
            "creating tmp table"), (
            "INSERT INTO tmp_test_table_latest\nSELECT id, name FROM test_table_staging\nWHERE version = :test_table_staging_version;\n",
            "copy from tmp"), (
            "BEGIN TRAN\n  EXEC sp_rename test_table_latest, test_table_latest_old;\n  EXEC sp_rename tmp_test_table_latest, test_table_latest;\n  DROP TABLE test_table_latest_old;\nCOMMIT",
            "copy from tmp and drop tmp database")]

        self.assertEqual(exp_tables, actual_tables)
        self.assertEqual(exp_queries, actual_queries)
        self.assertEqual(exp_tmp_tables, actual_variables)
