from unittest import TestCase
from unittest.mock import Mock

from commons.configurations.configurator import Configurator
from queries.query_plan_definitions import QueryPlanDef, Table, QueryDef
from transformers.partition_column_transformer import PartitionColumnTransformer


class TestPartitionColumnTransformer(TestCase):

    def setUp(self) -> None:
        self.config = Configurator().get_configurator()
        arks = {'query_single_result.return_value': 1234567}
        self.mock = Mock(**arks)
        empty_arks = {'query_single_result.return_value': None}
        self.empty_mock = Mock(**empty_arks)

    def test_transform(self):
        # Arrange
        partition = PartitionColumnTransformer(self.mock, self.config)
        query_plan_def = QueryPlanDef([Table('test_table_staging', 'version')], [QueryDef(
            "IF OBJECT_ID('dbo.tmp_test_table_latest', 'U') IS NOT NULL\nDROP TABLE dbo.tmp_test_table_latest;\n",
            'drop table if it exists'), QueryDef(
            "CREATE TABLE [sigma-dwh].dbo.tmp_test_table_latest (\n  id CHAR(36) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,\n  name NVARCHAR(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,\n  PRIMARY KEY (id));\n",
            "creating tmp table"),
            QueryDef(
                "INSERT INTO tmp_test_table_latest\nSELECT id, name FROM test_table_staging\nWHERE version = :test_table_staging_version;\n",
                "copy from tmp"),
            QueryDef(
                "BEGIN TRAN\n  EXEC sp_rename test_table_latest, test_table_latest_old;\n  EXEC sp_rename tmp_test_table_latest, test_table_latest;\n  DROP TABLE test_table_latest_old;\nCOMMIT",
                "copy from tmp and drop tmp database")], [])

        #Act
        actual = partition.transform(query_plan_def)
        
        # Assert
        actual_tables = list(map(lambda i: (i.name, i.partition_column), actual.tables))
        actual_queries = list(map(lambda i: (i.sql, i.description), actual.queries))

        exp_tables = [('test_table_staging', 'version')]
        exp_queries = [(
            "IF OBJECT_ID('dbo.tmp_test_table_latest', 'U') IS NOT NULL\nDROP TABLE dbo.tmp_test_table_latest;\n",
            'drop table if it exists'), (
            "CREATE TABLE [sigma-dwh].dbo.tmp_test_table_latest (\n  id CHAR(36) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,\n  name NVARCHAR(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,\n  PRIMARY KEY (id));\n",
            "creating tmp table"), (
            "INSERT INTO tmp_test_table_latest\nSELECT id, name FROM test_table_staging\nWHERE version = 1234567;\n",
            "copy from tmp"), (
            "BEGIN TRAN\n  EXEC sp_rename test_table_latest, test_table_latest_old;\n  EXEC sp_rename tmp_test_table_latest, test_table_latest;\n  DROP TABLE test_table_latest_old;\nCOMMIT",
            "copy from tmp and drop tmp database")]

        self.assertEqual(exp_tables, actual_tables)
        self.assertEqual(exp_queries, actual_queries)

    def test_transform_with_default_partition_value(self):
        config = self.config
        config.job.processing.sql_processor.default_partition_value = 1234567

        # Arrange
        partition = PartitionColumnTransformer(self.empty_mock, config)
        query_plan_def = QueryPlanDef([Table('test_table_staging', 'version')], [QueryDef(
            "IF OBJECT_ID('dbo.tmp_test_table_latest', 'U') IS NOT NULL\nDROP TABLE dbo.tmp_test_table_latest;\n",
            'drop table if it exists'), QueryDef(
            "CREATE TABLE [sigma-dwh].dbo.tmp_test_table_latest (\n  id CHAR(36) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,\n  name NVARCHAR(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,\n  PRIMARY KEY (id));\n",
            "creating tmp table"),
            QueryDef(
                "INSERT INTO tmp_test_table_latest\nSELECT id, name FROM test_table_staging\nWHERE version = :test_table_staging_version;\n",
                "copy from tmp"),
            QueryDef(
                "BEGIN TRAN\n  EXEC sp_rename test_table_latest, test_table_latest_old;\n  EXEC sp_rename tmp_test_table_latest, test_table_latest;\n  DROP TABLE test_table_latest_old;\nCOMMIT",
                "copy from tmp and drop tmp database")], [])

        # Act
        actual = partition.transform(query_plan_def)

        # Assert
        actual_tables = list(map(lambda i: (i.name, i.partition_column), actual.tables))
        actual_queries = list(map(lambda i: (i.sql, i.description), actual.queries))

        exp_tables = [('test_table_staging', 'version')]
        exp_queries = [(
            "IF OBJECT_ID('dbo.tmp_test_table_latest', 'U') IS NOT NULL\nDROP TABLE dbo.tmp_test_table_latest;\n",
            'drop table if it exists'), (
            "CREATE TABLE [sigma-dwh].dbo.tmp_test_table_latest (\n  id CHAR(36) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,\n  name NVARCHAR(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,\n  PRIMARY KEY (id));\n",
            "creating tmp table"), (
            "INSERT INTO tmp_test_table_latest\nSELECT id, name FROM test_table_staging\nWHERE version = 1234567;\n",
            "copy from tmp"), (
            "BEGIN TRAN\n  EXEC sp_rename test_table_latest, test_table_latest_old;\n  EXEC sp_rename tmp_test_table_latest, test_table_latest;\n  DROP TABLE test_table_latest_old;\nCOMMIT",
            "copy from tmp and drop tmp database")]

        self.assertEqual(exp_tables, actual_tables)
        self.assertEqual(exp_queries, actual_queries)

    def test_transform_without_default_partition_value(self):
        config = self.config
        config.job.processing.sql_processor.default_partition_value = None

        # Arrange
        partition = PartitionColumnTransformer(self.empty_mock, config)

        query_plan_def = QueryPlanDef([Table('test_table_staging', 'version')], [QueryDef(
            "IF OBJECT_ID('dbo.tmp_test_table_latest', 'U') IS NOT NULL\nDROP TABLE dbo.tmp_test_table_latest;\n",
            'drop table if it exists'), QueryDef(
            "CREATE TABLE [sigma-dwh].dbo.tmp_test_table_latest (\n  id CHAR(36) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,\n  name NVARCHAR(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,\n  PRIMARY KEY (id));\n",
            "creating tmp table"),
            QueryDef(
                "INSERT INTO tmp_test_table_latest\nSELECT id, name FROM test_table_staging\nWHERE version = :test_table_staging_version;\n",
                "copy from tmp"),
            QueryDef(
                "BEGIN TRAN\n  EXEC sp_rename test_table_latest, test_table_latest_old;\n  EXEC sp_rename tmp_test_table_latest, test_table_latest;\n  DROP TABLE test_table_latest_old;\nCOMMIT",
                "copy from tmp and drop tmp database")], [])

        # Act
        self.assertRaises(ValueError, lambda: partition.transform(query_plan_def))
