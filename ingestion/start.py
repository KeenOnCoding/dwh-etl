from pyspark.sql import SparkSession

from authorization.authorization_factory import AuthorizationFactory
from commons.db.query_runner import QueryRunner
from commons.db.sqlalchemyutils import create_engine
from commons.monitoring.prometheus_manager import PrometheusManager
from commons.spark.generic_stat_manager import GenericStatManager
from commons.spark.spark_db_sink import SparkDBSink
from commons.utils.stringconverters import dhrms_string_to_total_seconds
from commons.configurations.configurator import Configurator
from data_sources.data_source_factory import DataSourceFactory
from enrichers.db_schema_equalizer import DbSchemaEqualizer
from enrichers.enricher_chain import EnricherChain
from enrichers.field_mappings import FieldMappingsEnricher
from enrichers.hash_column_enricher import HashColumnEnricher
from enrichers.iso_date_aligner import ToDatetimeEnricher
from enrichers.json_stringify_enricher import JsonStringifyEnricher
from enrichers.list_parser_enricher import ListParserEnricher
from enrichers.partition_column_enricher import PartitionColumnEnricher
from enrichers.surrogate_key_enricher import SurrogateKeyEnricher
from enrichers.typecasters import ColumnTypeCasterEnricher
from enrichers.typecasters import TypeCasterFacade
from hash_sum_utils.datamanager import DataManager
from hash_sum_utils.hash_sum_extractor_factory import HashSumExtractorFactory
from hash_sum_utils.hashsumcheckers import HashSumCheckerFactory
from hash_sum_utils.requestparsers import FromToRequestParser
from ingestion.exec.exec_context_builder import ExecutionContextBuilder
from main import IngestionRunner
from query_params_builders.query_params_builder_factory import QueryParamsBuilderFactory
from commons.spark.connection_details import get_connection_details


def get_runner():
    config = Configurator().get_configurator()
    spark = SparkSession.builder.appName("dps-ingestion").getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    connection_details = get_connection_details(config)

    spark_db_sink = SparkDBSink(config, connection_details, spark)

    engine = create_engine(config)

    query_runner = QueryRunner(config, engine)

    query_params_builder = QueryParamsBuilderFactory(config).create()
    hash_sum_extractor_factory = HashSumExtractorFactory(config).create()

    util_hash_sum_table_name = config.job.ingestion.data_manager.hash_sum_table
    target_table = config.job.ingestion.general.target_table

    check_window = config.job.ingestion.data_manager.data_manager_window
    if check_window is None:
        check_window = '0d'

    data_manager_mode = config.job.ingestion.data_manager.date_manager_mode

    auth = AuthorizationFactory(config).create()

    hashsum_checker_factory = HashSumCheckerFactory(config,
                                                    query_params_builder=query_params_builder,
                                                    auth=auth)

    # well, I think it is a temporary solution with the FromToRequestParser
    # I think we should later introduce some kid of facade that is able to
    # generate different request parsers
    from_to_request_parser = FromToRequestParser(
        date_format=config.job.ingestion.general.date_format,
        from_to_param_names=config.job.ingestion.general.from_to_params_names
    )

    build_identifier = config.environment.shared.build_number, config.environment.shared.build_url

    if not all(build_identifier):
        raise RuntimeError('No BUILD_NUMBER or BUILD_URL')

    data_manager = DataManager(
        mode=data_manager_mode,
        query_runner=query_runner,
        target_table_name=target_table,
        hash_sum_table_name=util_hash_sum_table_name,
        hashsum_checker_factory=hashsum_checker_factory,
        window=check_window,
        request_parameter_parser=from_to_request_parser,
        direct_write=config.job.ingestion.data_manager.direct_write,  # config.config['DIRECT_WRITE'],
        build_identifier=build_identifier
    )

    batch_contexts = ExecutionContextBuilder(
        config=config,
        query_runner=query_runner,
    ).build()

    data_source = DataSourceFactory(config,
                                    query_runner,
                                    query_params_builder,
                                    hash_sum_extractor_factory,
                                    spark,
                                    auth=auth).create()

    partition_column_enricher = PartitionColumnEnricher(config)

    hash_column_enricher = HashColumnEnricher(config)

    list_parser_enricher = ListParserEnricher(config)

    field_mappings_enricher = FieldMappingsEnricher(config)

    db_schema_equalizer = DbSchemaEqualizer(config, query_runner)
    json_stringify_enricher = JsonStringifyEnricher(config)
    label = []
    env_label = config.environment.shared.env

    # extract lable from env vars to add it to metrics
    if env_label is None:
        env_label = 'prod'
    label.append(('env', env_label))
    label.append(('table', target_table))

    suppress_prometheus = config.environment.shared.suppress_prometheus == 'True'

    prometheus_manager = PrometheusManager(
        port=9091,
        labels=label,
        suppress=suppress_prometheus)

    spark_stat_manager = GenericStatManager(prometheus_manager, config)

    type_caster_facade = TypeCasterFacade(config)
    type_caster_enricher = ColumnTypeCasterEnricher(type_caster_facade,
                                                    config)

    iso_date_aligner = ToDatetimeEnricher(config)
    surrogate_key_enricher = SurrogateKeyEnricher(config)

    enricher_chain = EnricherChain()

    enricher_chain.add_enricher(field_mappings_enricher, 1)
    enricher_chain.add_enricher(list_parser_enricher, 2)
    enricher_chain.add_enricher(db_schema_equalizer, 3)  # equalize schema before calculating hash sum of each record
    enricher_chain.add_enricher(hash_column_enricher, 4)  # to prevent catching untracked fields in hash sum calculation
    enricher_chain.add_enricher(partition_column_enricher, 5)
    enricher_chain.add_enricher(surrogate_key_enricher, 6)
    # equalize schema again because hash, partition, or surrogate columns might be excessive
    enricher_chain.add_enricher(db_schema_equalizer, 7)

    enricher_chain.add_enricher(type_caster_enricher, 8)
    enricher_chain.add_enricher(json_stringify_enricher, 9)
    enricher_chain.add_enricher(iso_date_aligner, 10)

    from ingestion.pipeline.builder import EtlPipelineBuilder
    etl_pipeline_builder = EtlPipelineBuilder(sink=spark_db_sink,
                                              datasource=data_source,
                                              enricher_chain=enricher_chain)
    fetch_pause_raw = config.job.ingestion.general.fetch_pause  # config[FETCH_PAUSE]
    fetch_pause = dhrms_string_to_total_seconds(fetch_pause_raw) if fetch_pause_raw else 0

    return IngestionRunner(
        stat_manager=spark_stat_manager,
        batch_contexts=batch_contexts,
        data_manager=data_manager,
        etl_pipeline_builder=etl_pipeline_builder,
        fetch_pause=fetch_pause
    )


def main():
    runner = get_runner()
    runner.run()


if __name__ == '__main__':
    main()
