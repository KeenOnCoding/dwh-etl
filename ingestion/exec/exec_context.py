from abc import ABC, abstractmethod
from collections import UserDict

from ingestion.constants import BATCH_ID


class ExecutionContext(ABC, UserDict):

    def __init__(self):
        UserDict.__init__(self)

    def get_batch_id(self):
        return self.data[BATCH_ID]

    @abstractmethod
    def __hash__(self):
        pass

    def __eq__(self, other):
        if isinstance(other, ExecutionContext):
            return self.data == other.data

        return NotImplemented
