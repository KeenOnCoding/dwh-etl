from typing import List
from commons.logging.simple_logger import logger
from queries.query_plan_definitions import QueryPlanDef
from transformers.query_transformer import QueryTransformer

Query = str


class QueryBuilder:

    def __init__(self, transformers: List[QueryTransformer]):
        """ transformers -> List[QueryTransformer]"""

        self.transformers = transformers

    def createQuery(self, query_plan_def: QueryPlanDef) -> List[Query]:
        ret = []

        # transforming query plan
        for transformer in self.transformers:
            logger.info(f"Using query transformer: {type(transformer).__name__}")
            query_plan_def = transformer.transform(query_plan_def)

        # converting query plan into query object
        logger.info("Converting query plan into query object")
        for i in range(len(query_plan_def.queries)):
            ret.append(query_plan_def.queries[i].sql)
        return ret
