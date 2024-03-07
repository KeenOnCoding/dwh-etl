from ingestion.exec.exec_context import ExecutionContext
from ingestion.constants import BATCH_ID


class DummyExecutionContext(ExecutionContext):
    def __hash__(self):
        return hash(self.data[BATCH_ID])

    def __init__(self, batch_id):
        ExecutionContext.__init__(self)
        self.data[BATCH_ID] = batch_id

    def __repr__(self):
        return f'{self.__class__.__name__}({self.data})'
