from ingestion.constants import BATCH_ID
from ingestion.exec.exec_context import ExecutionContext


class SimpleIncrementalExecutionContextBuilder:
    @staticmethod
    def build(batch_id,
              date_from,
              date_to):
        return IncrementalExecutionContext(
            batch_id=batch_id,
            date_range=(date_from, date_to)
        )


class IncrementalExecutionContext(ExecutionContext):
    def __init__(self, batch_id: int,
                 date_range: tuple):
        ExecutionContext.__init__(self)
        self.data[BATCH_ID] = batch_id
        self.date_from, self.date_to = date_range
        self.date_range = date_range

    def get_context(self):
        return self.get_batch_id(), self.date_range

    def get_from_to_dates(self):
        return self.date_from, self.date_to

    def __hash__(self):
        return hash((self.date_from, self.date_to))

    def __eq__(self, other):
        if isinstance(other, IncrementalExecutionContext):
            return self.date_range == other.date_range

        return NotImplemented

    def __repr__(self):
        return f'{self.__class__.__name__}({dict(self.data, date_from=self.date_from, date_to=self.date_to)})'
