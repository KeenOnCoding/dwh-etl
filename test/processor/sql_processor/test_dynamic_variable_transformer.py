from unittest import TestCase
from unittest.mock import Mock
from commons.configurations.configurator import Configurator
from queries.query_plan_definitions import QueryPlanDef, Table, QueryDef
from transformers.dynamic_variable_transformer import DynamicVariableTransformer


class TestDynamicVariableTransformer(TestCase):
    def setUp(self) -> None:
        self.config = Configurator().get_configurator()
        self.config.job.processing.sql_processor.dynamic_variable = [
            {
                "variable": "max_batch_window",
                "value": "2419200"
            }
        ]
        self.dynamic_var = DynamicVariableTransformer(self.config)

    def test_transform(self):
        # Arrange
        query_plan_def = QueryPlanDef([Table('fact_time_reports', 'batch_id')], [QueryDef(
            "SELECT * FROM dbo.fact_time_reports_staging A INNER JOIN dbo.dim_projects_latest B ON (lower(A.project) = lower(B.project_code)) INNER JOIN dbo.dim_employees_latest C ON (  lower(A.employee_name) = lower(C.login_ad)) INNER JOIN dbo.dim_employees_latest D ON (lower(A.linear_manager_id) = lower(D.login_ad)) WHERE A.batch_id > :fact_time_reports_batch_id AND A.batch_id <= (:fact_time_reports_batch_id + :max_batch_window)",
            "processing fact and dimensional tables")], [])
        
         #Act
        actual = self.dynamic_var.transform(query_plan_def)

        #Assert
        actual_tables = list(map(lambda i: (i.name, i.partition_column), actual.tables))
        actual_queries = list(map(lambda i: (i.sql, i.description), actual.queries))

        exp_tables = [('fact_time_reports', 'batch_id')]
        exp_queries = [(
            "SELECT * FROM dbo.fact_time_reports_staging A INNER JOIN dbo.dim_projects_latest B ON (lower(A.project) = lower(B.project_code)) INNER JOIN dbo.dim_employees_latest C ON (  lower(A.employee_name) = lower(C.login_ad)) INNER JOIN dbo.dim_employees_latest D ON (lower(A.linear_manager_id) = lower(D.login_ad)) WHERE A.batch_id > :fact_time_reports_batch_id AND A.batch_id <= (:fact_time_reports_batch_id + 2419200)",
            "processing fact and dimensional tables")]

        self.assertEqual(exp_tables, actual_tables)
        self.assertEqual(exp_queries, actual_queries)
