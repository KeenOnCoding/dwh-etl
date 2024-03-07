from db.temporary import TemporaryTableManager
from commons.logging.simple_logger import logger


class DataManager:
    def __init__(self, query_runner):
        self.query_runner = query_runner

        self.temporary_table_manager: TemporaryTableManager = \
            TemporaryTableManager(self.query_runner)

    def create_temporary_table(self, table):
        temporary_target_table = self.temporary_table_manager.create_temporal(table)
        logger.info(f"Created temporary table '{temporary_target_table}' for table 'table'")
        return temporary_target_table

    def copy_data_to_table(self, target_table, temporary_table, start_date, end_date):
        self.query_runner.execute(f"""
            DELETE FROM {target_table} WHERE date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
        """)
        logger.info(f"Deleted data from '{target_table}' in range {start_date.date()} - {end_date.date()}")

        self.query_runner.execute(f"""
            INSERT INTO {target_table}
            SELECT * FROM {temporary_table}
        """)

    def insert_into_table(self, table, value):
        self.query_runner.execute(f"""
                INSERT INTO {table} VALUES {value}
            """)

    def remove_temporary_table(self):
        self.temporary_table_manager.delete_all()
