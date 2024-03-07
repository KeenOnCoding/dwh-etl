from abc import ABC, abstractmethod
from processor.sql_processor.queries.query_plan_definitions import QueryPlanDef


class QueryTransformer(ABC):

    @abstractmethod
    def transform(self, query_plan_def: QueryPlanDef) -> QueryPlanDef:
        pass
