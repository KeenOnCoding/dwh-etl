from unittest import TestCase
from unittest.mock import Mock

from commons.configurations.configurator import Configurator
from queries.query_builder import QueryBuilder
from queries.query_plan_definitions import QueryPlanDef, Table, QueryDef
from transformers.dynamic_variable_transformer import DynamicVariableTransformer
from transformers.partition_column_transformer import PartitionColumnTransformer


class TestQueryBuilder(TestCase):

    def setUp(self):
        self.config = Configurator().get_configurator()
        self.config.job.processing.sql_processor.dynamic_variable = [
            {
                "variable": "max_batch_window",
                "value": "2419200"
            }
        ]
        arks = {'query_single_result.return_value': 1234567}
        self.mock = Mock(**arks)

    def test_dimension_create_query(self):
        # Arrange
        query_builder = QueryBuilder([PartitionColumnTransformer(self.mock, self.config)])
        query_plan_def = QueryPlanDef([Table('test_table_staging', 'version')], [
            QueryDef(
            "IF OBJECT_ID('dbo.tmp_test_table_latest', 'U') IS NOT NULL\nDROP TABLE dbo.tmp_test_table_latest;\n",
            'drop table if it exists'), 
            QueryDef(
            "CREATE TABLE [sigma-dwh].dbo.tmp_test_table_latest (\n  id CHAR(36) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,\n  name NVARCHAR(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,\n  PRIMARY KEY (id));\n",
            "creating tmp table"),
            QueryDef(
                "INSERT INTO tmp_test_table_latest\nSELECT id, name FROM test_table_staging\nWHERE version = :test_table_staging_version;\n",
                "copy from tmp"),
            QueryDef(
                "BEGIN TRAN\n  EXEC sp_rename test_table_latest, test_table_latest_old;\n  EXEC sp_rename tmp_test_table_latest, test_table_latest;\n  DROP TABLE test_table_latest_old;\nCOMMIT",
                "copy from tmp and drop tmp database")], [])

        # Act
        actual = query_builder.createQuery(query_plan_def)

        # Assert
        exp = [
            "IF OBJECT_ID('dbo.tmp_test_table_latest', 'U') IS NOT NULL\nDROP TABLE dbo.tmp_test_table_latest;\n",
            "CREATE TABLE [sigma-dwh].dbo.tmp_test_table_latest (\n  id CHAR(36) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,\n  name NVARCHAR(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,\n  PRIMARY KEY (id));\n",
            "INSERT INTO tmp_test_table_latest\nSELECT id, name FROM test_table_staging\nWHERE version = 1234567;\n",
            "BEGIN TRAN\n  EXEC sp_rename test_table_latest, test_table_latest_old;\n  EXEC sp_rename tmp_test_table_latest, test_table_latest;\n  DROP TABLE test_table_latest_old;\nCOMMIT"]
        self.assertEqual(exp, actual)

    def test_fact_create_query(self):
        # Arrange
        query_builder = QueryBuilder([PartitionColumnTransformer(self.mock, self.config), DynamicVariableTransformer(config=self.config)])
        query_plan_def = QueryPlanDef([Table('fact_time_reports', 'batch_id')], [QueryDef(
            "SELECT * FROM dbo.fact_time_reports_staging A INNER JOIN dbo.dim_projects_latest B ON (lower(A.project) = lower(B.project_code)) INNER JOIN dbo.dim_employees_latest C ON (  lower(A.employee_name) = lower(C.login_ad)) INNER JOIN dbo.dim_employees_latest D ON (lower(A.linear_manager_id) = lower(D.login_ad)) WHERE A.batch_id > :fact_time_reports_batch_id AND A.batch_id <= (:fact_time_reports_batch_id + :max_batch_window)",
            "processing fact and dimensional tables")], [])

        # Act
        actual = query_builder.createQuery(query_plan_def)

        # Assert
        exp = [
            "SELECT * FROM dbo.fact_time_reports_staging A INNER JOIN dbo.dim_projects_latest B ON (lower(A.project) = lower(B.project_code)) INNER JOIN dbo.dim_employees_latest C ON (  lower(A.employee_name) = lower(C.login_ad)) INNER JOIN dbo.dim_employees_latest D ON (lower(A.linear_manager_id) = lower(D.login_ad)) WHERE A.batch_id > 1234567 AND A.batch_id <= (1234567 + 2419200)"]

        self.assertEqual(exp, actual)
