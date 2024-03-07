import time

from commons.logging.simple_logger import logger


class BatchIdProvider:
    GENERATED_BATCHES = set()

    @staticmethod
    def get_batch_id():
        batch_id = int(time.time())

        if batch_id in BatchIdProvider.GENERATED_BATCHES:
            # add length of set to prevent batch_id clashes
            dynamic_delta = len(BatchIdProvider.GENERATED_BATCHES)
            batch_id += dynamic_delta

        BatchIdProvider.GENERATED_BATCHES.add(batch_id)

        return batch_id
