from abc import ABC
from abc import abstractmethod


class QueryParamsBuilder(ABC):

    @abstractmethod
    def build(self, batch_context) -> str:
        pass
