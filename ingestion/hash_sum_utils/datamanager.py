import datetime
from enum import Enum
from typing import List

from ingestion.exec.exec_context import ExecutionContext
from commons.db.query_runner import QueryRunner
from commons.db.temporary import TemporaryTableManager
from ingestion.hash_sum_utils.hash_sum_manager import HashSumManager
from ingestion.hash_sum_utils.hashsumcheckers import HashSumChecker
from ingestion.hash_sum_utils.hashsumcheckers import HashSumCheckerFactory
from ingestion.hash_sum_utils.hashsumcheckers import HashSumCheckerResponse
from commons.logging.simple_logger import logger
from ingestion.constants import BATCH_ID


class DataManagerMode(Enum):
    INACTIVE = 'INACTIVE'
    SUBSTITUTE = 'SUBSTITUTE'


# TODO this is actually a god object,
class DataManager:
    """
    DataManager main purpose is to
    find data mismatches (e.g. some data can be added retroactively, so
    we need to check against changes like this and somehow update the data).

    So, when mode is set to SUBSTITUTE, it creates temporary tables in which
    the actual business data and utility data can be stored and letter data
    can be atomically copied within same transaction, implementing
    ALL OR NOTHING concept, so there is no inconsistencies.

    If you working with this class, all the external entities that are writing
    data to the target table, utility hash sum table should ask this class
    to give the target table name.

    If the mode is different from SUBSTITUTE, this class will return real
    table names, meaning that external entities will write to that tables
    immediately, and some methods of this class will do nothing
    """

    def __init__(self, *, hash_sum_table_name: str,
                 target_table_name: str,
                 mode: str,
                 query_runner: QueryRunner,
                 hashsum_checker_factory: HashSumCheckerFactory,
                 build_identifier,
                 window="0d",
                 request_parameter_parser=None,
                 batch_table="dbo.util_batch_track_table",
                 direct_write=None,
                 ):  # TODO pass batch table as a required argument

        self.build_number, self.build_url = build_identifier

        self.batch_table = batch_table
        self.direct_write = direct_write == 'true'
        self.window = window
        self.request_parameter_parser = request_parameter_parser

        self.hashsum_checker_factory = hashsum_checker_factory
        self.real_hash_sum_manager = HashSumManager(hash_sum_table=hash_sum_table_name,
                                                    target_table=target_table_name,
                                                    query_runner=query_runner,
                                                    window=window,
                                                    request_parameter_parser=request_parameter_parser)

        self.temporary_table_manager: TemporaryTableManager = \
            TemporaryTableManager(query_runner)

        self.query_runner = query_runner
        try:
            self.mode = DataManagerMode[mode]
            logger.info(f'Using DataManager in {self.mode} mode')
        except KeyError:
            # default mode is INACTIVE
            if mode is None:
                self.mode = DataManagerMode.INACTIVE
                logger.info('Using DataManager in INACTIVE mode')
            else:
                raise RuntimeError(f'No such mode [{mode}]. '
                                   f'To disable watchdog set explicitly '
                                   f'status to "INACTIVE"')

        self.hash_sum_table_name: str = hash_sum_table_name
        self.target_table_name: str = target_table_name
        logger.info(f"Using hash_sum_table_name: {self.hash_sum_table_name}")
        logger.info(f"Using target_table_name: {self.target_table_name}")

        self.temporals_initialized: bool = False

        # placeholders that will be initialized later
        self.temporary_target_table_name: str = ""
        self.temporary_hash_sum_table_name: str = ""
        self.temporary_hash_sum_manager: HashSumManager = None

    def save_hash_sum_sql(self):
        logger.info(f"Saving hash sum: "
                    f"copy data from '{self.temporary_hash_sum_table_name}' "
                    f"to '{self.hash_sum_table_name}'")

        save_hash_sum_sql = f"""
                        DELETE FROM {self.hash_sum_table_name}
                        WHERE request_parameters IN
                            (SELECT DISTINCT request_parameters
                            FROM {self.temporary_hash_sum_table_name})
                        AND table_name = '{self.target_table_name}';
                        ---
                        INSERT INTO {self.hash_sum_table_name}
                            (batch_id,
                             table_name,
                             request_parameters,
                             hash_sum,
                             created_timestamp)
                        SELECT batch_id,
                               table_name,
                               request_parameters,
                               hash_sum,
                               created_timestamp
                        FROM {self.temporary_hash_sum_table_name};
                """
        return save_hash_sum_sql

    def copy_to_staging(self):
        logger.info(f"Saving data to staging table: "
                    f"copy data from '{self.temporary_target_table_name}' "
                    f"to '{self.target_table_name}'")
        copy_to_staging = f"""
                        {self.save_hash_sum_sql() if self.hash_sum_table_name is not None else ""}

                        INSERT INTO {self.target_table_name}
                        SELECT * FROM {self.temporary_target_table_name}
                """
        return copy_to_staging

    def transactional_save(self, min_batch, max_batch) -> None:
        logger.info(f"Transaction saving")
        logger.info(f"Update batch table: '{self.batch_table}'")
        sql = f"""                    
                INSERT INTO {self.batch_table} 
                (date, table_name, min_batch, max_batch, is_processed, ingestion_build_number, ingestion_build_url)
                VALUES ('{datetime.datetime.now()}',
                        '{self.target_table_name}',
                        {min_batch},
                        {max_batch}, 
                        0,
                        {self.build_number},
                        '{self.build_url}');      
    
                {"" if self.direct_write else self.copy_to_staging()}
               """
        self.query_runner.execute(sql)

    def scan_for_mismatched_contexts(self) -> List[ExecutionContext]:
        if self.mode == DataManagerMode.INACTIVE:
            return []

        from ingestion.exec.batch import BatchIdProvider
        hash_sum_records = self.real_hash_sum_manager.load(BatchIdProvider.get_batch_id())
        contexts = []

        checker: HashSumChecker = self.hashsum_checker_factory.create()
        for record in hash_sum_records:
            response: HashSumCheckerResponse = \
                checker.digest(record).check()

            if response.is_modified:
                ctx = response.context
                ctx[BATCH_ID] = BatchIdProvider.get_batch_id()
                contexts.append(response.context)

        logger.info(f'Found {len(contexts)} mismatched contexts')
        return contexts

    def init(self) -> None:
        # prevent reinitialization of the tables
        if self.temporals_initialized or self.direct_write:
            return

            # temporary table for placing data records
        self.temporary_target_table_name = self.temporary_table_manager.create_temporal(self.target_table_name)
        logger.info(f"Temporary target table: '{self.temporary_target_table_name}'")
        logger.info(f"Hash sum table name: '{self.hash_sum_table_name}'")

        # temporary table for storing hash sums
        if self.hash_sum_table_name is None:
            # TODO proper temporary hashsum table handling when no hashsum table supplied
            self.temporary_hash_sum_table_name = None
        else:
            self.temporary_hash_sum_table_name = self.temporary_table_manager.create_temporal(self.hash_sum_table_name)
            logger.info(f"Creating temporary table for storing hash sums. "
                        f"Temporary hash sum table name: '{self.temporary_hash_sum_table_name}'")

        # hash sum manager for writing hash sums into temporal table
        self.temporary_hash_sum_manager = HashSumManager(
            hash_sum_table=self.temporary_hash_sum_table_name,
            target_table=self.target_table_name,
            query_runner=self.query_runner,
            window=self.window,
            request_parameter_parser=self.request_parameter_parser
        )

        self.temporals_initialized = True

    def save_data(self, **kwargs) -> None:
        self.transactional_save(**kwargs)

    def get_target_table(self) -> str:
        return self.target_table_name if self.direct_write else self.temporary_target_table_name

    def get_hash_sum_manager(self) -> HashSumManager:
        logger.info("Getting hash sum manager")
        return self.real_hash_sum_manager if self.direct_write else self.temporary_hash_sum_manager

    def cleanup(self) -> None:
        logger.info("Deleting temporary table manager")
        self.temporary_table_manager.delete_all()
