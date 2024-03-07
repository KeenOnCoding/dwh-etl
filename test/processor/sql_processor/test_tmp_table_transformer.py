import pytest
import unittest
from unittest.mock import patch

from commons.configurations.configurator import Configurator
from processor.sql_processor.queries.query_plan_definitions import QueryDef, QueryPlanDef, Table
from processor.sql_processor.transformers.tmp_table_transformer import TmpTableTransformer


@pytest.mark.skip(reason='Require rework (new logic was introduced in commons package')
class TestTmpTableTransformer(unittest.TestCase):

    @patch('sqlalchemy.schema.Table.__init__')
    def setUp(self, table) -> None:
        self.config = Configurator().get_configurator()
        self.config.job.processing.sql_processor.tables_to_rewrite = ['dbo.test_table_latest']

        config_without_schema = self.config
        config_without_schema.job.processing.sql_processor.tables_to_rewrite = ['test_table_latest']
        self.config_without_schema = config_without_schema
        table.return_value = None

    def test_transform(self):
        # Arrange
        tmp_table = TmpTableTransformer(None, None, self.config)
        query_plan_def = QueryPlanDef(
            [Table('test_table_staging', 'version')],
            [
                QueryDef
                ("INSERT INTO dbo.test_table_latest\nSELECT id, name FROM test_table_staging\nWHERE version = "
                 "1234567;\n",
                 "copy from tmp")
            ],
            {'TABLES_TO_REWRITE': ['dbo.test_table_latest']})

        # Act
        actual = tmp_table.transform(query_plan_def)

        # Assert
        actual_tables = list(map(lambda i: (i.name, i.partition_column), actual.tables))
        actual_queries = list(map(lambda i: (i.sql, i.description), actual.queries))
        actual_variables = actual.variables

        exp_variables = {'TABLES_TO_REWRITE': ['dbo.test_table_latest']}
        exp_tables = [('test_table_staging', 'version')]
        exp_queries = [
            (
                "\n\nIF OBJECT_ID('dbo.tmp_test_table_latest', 'U') IS NOT NULL\n\tDROP TABLE dbo.tmp_test_table_latest;\n",
                "drop table if it exists"),
            ("\nCREATE TABLE dbo.tmp_test_table_latest (\n)\n\n", "creating tmp table"),
            ("INSERT INTO dbo.tmp_test_table_latest\nSELECT id, name "
             "FROM test_table_staging\nWHERE version = 1234567;\n",
             "copy from tmp"),
            ("\nBEGIN TRAN\nEXEC sp_rename 'dbo.test_table_latest', test_table_latest_old;\n"
             "EXEC sp_rename 'dbo.tmp_test_table_latest', test_table_latest;\n"
             "DROP TABLE dbo.test_table_latest_old;\nCOMMIT\n",
             "copy from tmp and drop tmp database")]

        self.assertEqual(exp_tables, actual_tables)
        self.assertEqual(exp_queries, actual_queries)
        self.assertEqual(exp_variables, actual_variables)

    def test_transform_without_table_schema(self):
        # Arrange
        tmp_table = TmpTableTransformer(None, None, self.config_without_schema)
        query_plan_def = QueryPlanDef([Table('test_table_staging', 'version')], [QueryDef(
            "INSERT INTO dbo.test_table_latest\nSELECT id, name FROM test_table_staging\nWHERE version = 1234567;\n",
            "copy from tmp")], {'TABLES_TO_REWRITE': ['dbo.test_table_latest']})

        # Assert
        self.assertRaises(Exception, lambda _: tmp_table.transform(query_plan_def))
