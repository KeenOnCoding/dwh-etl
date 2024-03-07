import copy

from commons.logging.simple_logger import logger
from queries.query_plan_definitions import QueryPlanDef
from transformers.query_transformer import QueryTransformer


class DynamicVariableTransformer(QueryTransformer):

    def __init__(self, config):
        self.config = config

    def transform(self, query_plan_def: QueryPlanDef) -> QueryPlanDef:
        dynamic_variables = self.config.job.processing.sql_processor.dynamic_variable

        # if table object is not defined inside plan then just exit
        if dynamic_variables is None:
            return query_plan_def

        plan = copy.deepcopy(query_plan_def)

        # iterating through possible variables
        for variable in dynamic_variables:
            # building variable name
            var_name = variable['variable']
            value = variable['value']

            # trying to find it inside queries
            logger.info(f"Trying to find {var_name} in query")
            for query in range(len(plan.queries)):
                query_sql = plan.queries[query].sql

                if var_name in query_sql:
                    plan.queries[query].sql = query_sql \
                        .replace(":{}".format(var_name), value) \
                        .replace("{{{}}}".format(var_name), value)
                    logger.info(f"Replaced in query {var_name} with {str(value)}")

            # trying to find it inside queries
            for query in range(len(plan.tables)):
                name = plan.tables[query].name

                if var_name in name:
                    plan.tables[query].name = \
                        name.replace(":{}".format(var_name), value)

            if 'TABLES_TO_REWRITE' in plan.variables:
                for var in range(len(plan.variables['TABLES_TO_REWRITE'])):
                    variables = plan.variables['TABLES_TO_REWRITE'][var]

                    if var_name in variables:
                        plan.variables['TABLES_TO_REWRITE'][var] =\
                            variables.replace(":{}".format(var_name), value)

        logger.info(f"Tables was configured with parameters")
        for tab in plan.tables:
            logger.info(f"Table: {tab.name}. Partition column: {tab.partition_column}")

        return plan
