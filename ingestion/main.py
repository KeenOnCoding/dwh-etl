import pprint
import time
from typing import List

from commons.logging.simple_logger import logger
from hash_sum_utils.datamanager import DataManager
from ingestion.exec.exec_context import ExecutionContext
from ingestion.constants import IN_AMOUNT, OUT_AMOUNT, HASH_SUM_MANAGER, OUT_TABLE, BATCH_ID
from ingestion.pipeline.builder import EtlPipelineBuilder


class IngestionException(Exception):
    pass


class IngestionRunner:

    def __init__(self, *, stat_manager,
                 batch_contexts,
                 data_manager,
                 etl_pipeline_builder,
                 fetch_pause):
        self.stat_manager = stat_manager

        self.batch_contexts: List[ExecutionContext] = batch_contexts
        self.data_manager: DataManager = data_manager
        self.etl_pipeline_builder: EtlPipelineBuilder = etl_pipeline_builder

        self.fetch_pause = fetch_pause

    def run(self):
        try:
            self.data_manager.init()
            mismatched_contexts = self.data_manager.scan_for_mismatched_contexts()
            logger.info(f"Found mismatched contexts: {mismatched_contexts}")
            all_contexts = mismatched_contexts + self.batch_contexts

            unique_contexts = set(all_contexts)
            target_table = self.data_manager.get_target_table()
            logger.info(f"Target table: {target_table}")

            hash_sum_manager = self.data_manager.get_hash_sum_manager()

            etl_pipeline = self.etl_pipeline_builder.build()

            shared_runtime_config = {
                HASH_SUM_MANAGER: hash_sum_manager,
                OUT_TABLE: target_table
            }
            ingestion_stats = list()

            for context in unique_contexts:
                logger.info(f'Running with context: {context}')

                etl_context = context
                etl_context.update(shared_runtime_config)

                is_err, res = etl_pipeline.invoke(context=etl_context)
                if is_err:
                    raise IngestionException from res

                in_amount, out_amount, is_err = etl_context[IN_AMOUNT], etl_context[OUT_AMOUNT], is_err
                batch_id = etl_context[BATCH_ID]
                ingestion_stats.append((in_amount, out_amount, is_err, batch_id, etl_context))

                time.sleep(self.fetch_pause)
        except IngestionException as exc:
            raise RuntimeError from exc
        else:
            self.handle_stats(ingestion_stats)
            import operator
            batch_id_indexer = operator.itemgetter(3)
            batches = [batch_id_indexer(stat) for stat in ingestion_stats]
            if batches:
                min_batch = min(batches)
                max_batch = max(batches)

                self.data_manager.save_data(min_batch=min_batch,
                                            max_batch=max_batch)
        finally:
            self.data_manager.cleanup()

    def handle_stats(self, ingestion_stats):
        logger.info('Ingestion statistics:')
        total_in = 0
        total_out = 0
        total_is_err = False
        batches = []
        for in_count, out_count, is_err, batch_id, ctx in ingestion_stats:
            self.stat_manager.handle_numerical_statistics(rec_in=in_count,
                                                          rec_out=out_count,
                                                          has_err=is_err)
            logger.info(f'[{batch_id}]: in - {in_count}, out - {out_count}, is err - {is_err}')

            total_in += in_count
            total_out += out_count
            total_is_err |= is_err
            batches.append(batch_id)

        logger.info(f"Total in: {total_in} \n"
                    f"Total out: {total_out} \n"
                    f"Error: {total_is_err} \n"
                    f"Generated batches: {batches} \n")
