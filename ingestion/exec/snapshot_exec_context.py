from ingestion.exec.exec_context import ExecutionContext
from ingestion.constants import BATCH_ID


class SnapshotExecutionContext(ExecutionContext):

    def __init__(self, batch_id):
        ExecutionContext.__init__(self)
        self.data[BATCH_ID] = batch_id

    def __hash__(self):
        return hash(self.data[BATCH_ID])

    def __repr__(self):
        return f'{self.__class__.__name__}({self.data})'
