import copy
from abc import ABC
from enum import Enum, auto

from commons.db.query_runner import QueryRunner
from commons.logging.simple_logger import logger
from processor.sql_processor.queries.query_plan_definitions import QueryPlanDef
from processor.sql_processor.transformers.query_transformer import QueryTransformer


class TransformerConstants(Enum):
    MIN_BATCHES_TO_PROCESS = auto()


class StaticTransformerExtension(QueryTransformer, ABC):
    pass


class RedundantQueryRemover(StaticTransformerExtension):
    """
    RedundantQueryRemover removes all the queries if the value propagated by RaiseOnNoBatchToProcessExtension
    is equal to zero, meaning that the defined queries should not be run
    """

    def __init__(self, cfg):
        self.cfg = cfg

    def transform(self, query_plan_def: QueryPlanDef) -> QueryPlanDef:
        min_batch_to_process = self.cfg.job.processing.sql_processor.min_batches_to_process = TransformerConstants.MIN_BATCHES_TO_PROCESS
        logger.info(f"Using min_batch_to_process equal to {min_batch_to_process}")
        if min_batch_to_process == 0:
            logger.info("Removing all queries")
            query_plan_def.queries = []

        return query_plan_def


class CleanupExtension(StaticTransformerExtension):

    def transform(self, query_plan_def: QueryPlanDef) -> QueryPlanDef:
        for query in query_plan_def.queries:
            tables_to_cleanup = query.tables_to_cleanup
            logger.info(f"Using table to cleanup {tables_to_cleanup}")
            if tables_to_cleanup:
                drop_queries = []
                for table in tables_to_cleanup:
                    drop_queries.append(f'DROP TABLE {table};')

                query.sql += '\n' + '\n'.join(drop_queries)

        return query_plan_def


class RaiseOnNoBatchToProcessExtension(StaticTransformerExtension):
    """
    RaiseOnNoBatchToProcessExtension checks how many batches left to process
    in each staging table annotated in processing script.

    By default, if there PLAN contains at least 1 staging table that lacks batches to process
    the exception will be raised. This behaviour can be adjusted using RAISE_WHEN_NO_BATCHES_TO_PROCESS parameter.

    If the PLAN contains no batch filter, then no exception will be raised, and -1 value will be propagated to the
    configuration.
    """

    def __init__(self, cfg, raise_on_no_batches_to_process: bool, query_runner: QueryRunner,
                 batch_table: str = 'dbo.util_batch_track_table'):
        self.cfg = cfg
        self.raise_on_no_batches_to_process = raise_on_no_batches_to_process
        self.query_runner = query_runner
        self.batch_table = batch_table

    def transform(self, query_plan_def: QueryPlanDef) -> QueryPlanDef:
        unprocessed_batches = []
        for query in query_plan_def.queries:
            staging_table = query.staging_table
            logger.info(f"Staging table: {staging_table}")
            if staging_table is not None:
                amount_unprocessed = self.query_runner.query_single_result(
                    f"""SELECT COUNT(*) FROM {self.batch_table}
                        WHERE table_name = '{staging_table}'
                            AND is_processed = 0
                    """)
                unprocessed_batches.append(amount_unprocessed)

        try:
            min_batch_to_process = min(unprocessed_batches)
            logger.info(f"Min batch to process {min_batch_to_process}")
        except ValueError:
            logger.info(f'Current PLAN does not use {BatchHighLowConditionFilter.__name__}')
            min_batch_to_process = -1  # designate that the plan does not use batch filter

        self.cfg.job.processing.sql_processor.min_batches_to_process = min_batch_to_process
        logger.info(f"Update configuration parameter TransformerConstants.MIN_BATCHES_TO_PROCESS"
                    f" with min_batch_to_process = {min_batch_to_process} value")
        if min_batch_to_process == 0 and self.raise_on_no_batches_to_process:
            raise RuntimeError('There are no batches to process')

        return query_plan_def


class CatchupExtension(StaticTransformerExtension):
    """
      CatchupExtension replicates the current PLAN N-times, where N > 1, minimal amount of batches to process.
      If N = 0 - removes all the queries from the query plan definition.
      If N = -1 (case with script without batch filter usage) - leave plan untouched.
      """

    def __init__(self, cfg, use_catchup):
        self.cfg = cfg
        self.use_catchup = use_catchup

    def transform(self, query_plan_def: QueryPlanDef) -> QueryPlanDef:
        if not self.use_catchup:
            return query_plan_def

        min_batch_to_process = self.cfg.job.processing.sql_processor.min_batches_to_process
        logger.info(f'Running PLAN_ID with CATCHUP {min_batch_to_process} times')

        if min_batch_to_process > 1:
            query_copy = copy.deepcopy(query_plan_def.queries)
            query_plan_def.queries.extend(query_copy * (min_batch_to_process - 1))

        return query_plan_def


class ConsolePrinter(StaticTransformerExtension):
    OUTLINE_LEN = 200
    OUTLINE = "*" * OUTLINE_LEN

    def transform(self, query_plan_def: QueryPlanDef) -> QueryPlanDef:
        for query in query_plan_def.queries:
            logger.info(f'{self.OUTLINE}\n'
                        f'{query}\n'
                        f'{self.OUTLINE}')
        return query_plan_def


class BatchHighLowConditionFilter(StaticTransformerExtension):
    FILTER_ANCHOR = '{BATCH_HIGH_LOW_FILTER}'

    PRECONDITION_QUERY = """
        DECLARE @high_batch_id BIGINT
        DECLARE @low_batch_id BIGINT
        SELECT @high_batch_id = max_batch, 
               @low_batch_id = min_batch
        FROM {batch_table}
        WHERE table_name = '{table_name}'
            AND date = (SELECT MIN(date) FROM {batch_table}
                        WHERE table_name = '{table_name}'
                        AND is_processed = 0);

    """
    FILTER_CONDITION_QUERY = "{table_alias}{partition_column} >= @low_batch_id" \
                             " AND {table_alias}{partition_column} <= @high_batch_id"

    MARK_PROCESSED_QUERY = """
        UPDATE {batch_table}
        SET is_processed = 1,
            processor_build_number = '{build_number}',
            processor_build_url = '{build_url}'
        WHERE table_name = '{table_name}' AND max_batch = @high_batch_id AND min_batch = @low_batch_id;
    """

    def __init__(self, build_identifier, batch_table='dbo.util_batch_track_table'):
        self.batch_table = batch_table
        self.build_number, self.build_url = build_identifier

    def transform(self, query_plan_def: QueryPlanDef) -> QueryPlanDef:
        for ix, query in enumerate(query_plan_def.queries):
            # TODO maybe filter can be expressed directly in plan file instead of
            #   searching it in the SQL
            if self.FILTER_ANCHOR in query.sql:
                logger.info(f"Using FILTER_ANCHOR: {self.FILTER_ANCHOR}")

                # TODO this checks should not be part a part of transform
                #  ideally, no checks should be required, the reader should somehow understand if the plan requires
                #  this parameters
                if not query.staging_table:
                    raise ValueError('No staging table name found. Please, add "staging-table" parameter in PLAN file')

                if not query.partition_column:
                    raise ValueError('No partition column declaration found. Please add "partition-column" parameter '
                                     'in PLAN file')

                staging_table = query.staging_table
                logger.info(f"Founded staging table: {staging_table}")
                logger.info(f"Batch table: {self.batch_table}")

                precondition_sql = self.PRECONDITION_QUERY.format(
                    batch_table=self.batch_table,
                    table_name=staging_table,
                )

                self.check_staging(staging_table, query.sql)
                logger.info(f"Found staging tables '{staging_table}' in SQL script")

                # add dot to generify cases when no alias used
                condition_alias = f'{query.staging_table_alias}.' if query.staging_table_alias else ""
                logger.info(f"Using condition alias: '{condition_alias}'")

                condition_query = self.FILTER_CONDITION_QUERY.format(partition_column=query.partition_column,
                                                                     table_alias=condition_alias)
                logger.info(f"Using condition query: {condition_query}")

                mark_query = self.MARK_PROCESSED_QUERY.format(batch_table=self.batch_table,
                                                              table_name=staging_table,
                                                              build_number=self.build_number,
                                                              build_url=self.build_url)
                updated_sql = precondition_sql + query.sql.replace(self.FILTER_ANCHOR, condition_query) + mark_query
                logger.info(f"Updated SQL query. Replaced FILTER_ANCHOR with condition query.")

                query.sql = updated_sql
                logger.info(f"Query was updated")
        return query_plan_def

    @staticmethod
    def check_staging(staging_table: str, query: str):
        """
        When using condition filter we assume that in SQL script name of the staging table
        will be always present, because we use `SELECT ... FROM staging_table`.

        Raises exception when wrong staging table was typed in query parameters.
        """
        if staging_table not in query:
            raise ValueError(f'{staging_table} not found in SQL.')
