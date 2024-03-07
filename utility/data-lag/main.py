from lag.lag_checker import NoDataException
from commons.utils.stringconverters import mnemonic_to_absolute_sec


class DataLagRunner:
    __slots__ = ('config', 'lag_checker', 'engine', 'prometheus_manager',
                 'rule_reader', 'rule_plan_builder', 'rule_plan_executor',
                 'metric_name')

    def __init__(self, *, config, engine, lag_checker,
                 prometheus_manager, rule_reader, rule_plan_builder,
                 rule_plan_executor, metric_name):
        self.config = config
        self.lag_checker = lag_checker
        self.engine = engine
        self.prometheus_manager = prometheus_manager
        self.rule_reader = rule_reader
        self.rule_plan_builder = rule_plan_builder
        self.rule_plan_executor = rule_plan_executor
        self.metric_name = metric_name

    def run(self):
        schemas = self.rule_reader.get_schemas()
        schema_to_tables = dict()
        empty_schemas = list()

        # map schema to tables within it
        for schema in schemas:
            table_names = self.engine.table_names(schema=schema)
            if len(table_names) == 0:
                empty_schemas.append(schema)
            schema_to_tables[schema] = table_names

        if len(empty_schemas) != 0:
            raise RuntimeError(f"These schemas {empty_schemas} do not contain "
                               f"any tables. Please consider using "
                               f"schemas with at least 1 existing table")

        for schema, tables in schema_to_tables.items():
            # retrieve raw plan, prepare transform plan and apply to tables
            rule_raw_plan = self.rule_reader.get_schema_rule_plan(schema)
            plan = self.rule_plan_builder.build_plan(rule_raw_plan)

            lag_payloads = self.rule_plan_executor.apply_rules(schema, tables, plan)

            for lag_payload in lag_payloads:
                opt_label = [('table', "{}.{}".format(lag_payload.schema, lag_payload.table_name))]
                try:
                    check_response = self.lag_checker.check(lag_payload)
                    response_description = check_response.get_description()
                    lag = check_response.lag()

                except NoDataException as e:
                    print(e)
                    # extract the threshold and send incremented
                    # to indicate the problem
                    lag = mnemonic_to_absolute_sec(lag_payload.threshold_value) + 1

                self.prometheus_manager.send_counter(
                    f"data-lag-{lag_payload.threshold_value}",
                    self.metric_name,
                    'Number of data lag inside staging and dwh tables',
                    lag,
                    opt_label=opt_label
                )
