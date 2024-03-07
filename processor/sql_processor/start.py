from commons.db.query_runner import QueryRunner
from commons.db.sqlalchemyutils import create_engine
from commons.configurations.configurator import Configurator
from processor.sql_processor.main import SqlProcessorRunner
from queries.query_builder import QueryBuilder
from queries.query_plan_def_reader import QueryPlanDefReader
from transformers.dynamic_variable_transformer import DynamicVariableTransformer
from transformers.partition_column_transformer import PartitionColumnTransformer
from transformers.static_transformer import BatchHighLowConditionFilter, ConsolePrinter
from transformers.static_transformer import CatchupExtension
from transformers.static_transformer import CleanupExtension
from transformers.static_transformer import RaiseOnNoBatchToProcessExtension
from transformers.static_transformer import RedundantQueryRemover
from transformers.tmp_table_transformer import TmpTableTransformer


def get_runner():
    config = Configurator().get_configurator()

    use_catchup = config.job.processing.sql_processor.catchup
    raise_on_no_batch_to_proc = config.job.processing.sql_processor.raise_when_no_batches_to_process is not False

    build_number = config.environment.shared.build_number
    build_url = config.environment.shared.build_url
    build_identifier = build_number, build_url

    if not all(build_identifier):
        raise RuntimeError('No build_number or build_url set')

    engine = create_engine(config)

    query_runner = QueryRunner(config, engine)

    query_plan_def_reader = QueryPlanDefReader(config)

    transformers = [
        DynamicVariableTransformer(config),
        PartitionColumnTransformer(query_runner, config),
        TmpTableTransformer(engine, query_runner, config),
        RaiseOnNoBatchToProcessExtension(config, raise_on_no_batch_to_proc, query_runner),
        BatchHighLowConditionFilter(build_identifier),
        CleanupExtension(),
        ConsolePrinter(),
        CatchupExtension(config, use_catchup),
        RedundantQueryRemover(config)
    ]

    query_builder = QueryBuilder(transformers)

    return SqlProcessorRunner(query_plan_def_reader, query_builder,
                              query_runner, config)


def main():
    runner = get_runner()
    runner.run()


if __name__ == "__main__":
    main()
