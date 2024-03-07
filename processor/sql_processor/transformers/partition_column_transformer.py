# PartitionColumnTransformer
import copy
import warnings

from queries.query_plan_definitions import QueryPlanDef
from transformers.query_transformer import QueryTransformer
from commons.logging.simple_logger import logger


class PartitionColumnTransformer(QueryTransformer):

    def __init__(self, query_runner, config):
        self.query_runner = query_runner
        self.config = config

    def transform(self, query_plan_def: QueryPlanDef) -> QueryPlanDef:
        # if table object is not defined inside plan then just exit
        if query_plan_def.tables is None:
            logger.info("Table object is not defined inside plan")
            return query_plan_def

        plan = copy.deepcopy(query_plan_def)
        # iterating through possible variables
        for tables in range(len(plan.tables)):
            # building variable name
            try:
                variable = (":" + plan.tables[tables].name + "_" +
                            plan.tables[tables].partition_column)
                # cretign empty value and using it as lazy initializer
                logger.info(f"Using plan variable: '{variable}'")
            except TypeError as e:
                warnings.warn(str(e))
                continue

            value = None

            # trying to find it inside queries
            for query in range(len(plan.queries)):
                query_sql = plan.queries[query].sql

                if variable in query_sql:
                    if value is None:
                        value = self.__read_variable(
                            plan.tables[tables].name,
                            plan.tables[tables].partition_column)
                        logger.info(f"Using plan variable [{variable}] "
                                    f"found with value [{value}]")
                    plan.queries[query].sql = query_sql.replace(variable, str(value))
                    logger.info(f"Replaced in query {variable} with {str(value)}")

        return plan

    def __read_variable(self, table, part_column) -> int:
        part_column = self.query_runner \
            .query_single_result(f"select max({part_column}) from {table}")

        if part_column is None:
            part_column = self.config.job.processing.sql_processor.default_partition_value
            if part_column is None:
                raise ValueError("DEFAULT_PARTITION_VALUE is not set.")
            logger.info(f"Using DEFAULT_PARTITION_VALUE: {part_column}")
        logger.info(f"Using as partition value {part_column}")
        return part_column
