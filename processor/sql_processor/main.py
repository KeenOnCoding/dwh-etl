from commons.logging.simple_logger import logger

class SqlProcessorRunner:

    def __init__(self, query_plan_def_reader, query_builder,
                 query_runner, config):
        self.query_builder = query_builder
        self.query_runner = query_runner
        self.query_plan_def_reader = query_plan_def_reader
        self.config = config

    def run(self):
        plan_id = self.config.job.processing.sql_processor.plan_id

        # reading query plan
        query_plan_def = self.query_plan_def_reader.read(plan_id)

        # transforming query def and building queries
        queries = self.query_builder.createQuery(query_plan_def)

        # running queries
        logger.info("Running all queries")
        for query in queries:
            self.query_runner.execute(query)
