from commons.logging.simple_logger import logger


class QueryRunner:

    def __init__(self, config, engine):
        self.engine = engine
        self.config = config

    def execute(self, query: str, *, verbose: bool = False):
        if verbose:
            logger.info(f'Running query: {query}')
        connection = self.engine.raw_connection()
        try:
            with connection.cursor() as cursor:
                logger.info(f"Query for execution: {query}")
                cursor.execute(query)
                while cursor.nextset():
                    ...
                connection.commit()
            logger.info("Query was executed")
        except Exception as exc:
            logger.error(str(exc))
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()
            logger.info("Connection with database was closed")

    def query_single_result(self, query: str, isolation_level="AUTOCOMMIT", *, verbose: bool = False):
        if verbose:
            logger.info(f'Running query: {query}.\n'
                        f'Isolation level: {isolation_level}')

        with self.engine.connect().execution_options(
                isolation_level=isolation_level) as connection:
            result = connection.execute(query)
            single_row = result.fetchall()

            return single_row[0][0] if single_row else None

    def query(self, query: str, isolation_level="AUTOCOMMIT", *, verbose: bool = False):
        if verbose:
            logger.info(f'Running query: {query}.\n'
                        f'Isolation level: {isolation_level}')

        with self.engine.connect().execution_options(
                isolation_level=isolation_level) as connection:
            result = connection.execute(query)
            return list(result)
