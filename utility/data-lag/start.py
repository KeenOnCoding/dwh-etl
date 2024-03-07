import warnings

from commons.db.query_runner import QueryRunner
from commons.db.sqlalchemyutils import create_engine
from commons.monitoring.prometheus_manager import PrometheusManager
from configurations.configurator import Configurator
from lag.lag_checker import LagChecker
from main import DataLagRunner
from rule.rule_plan import RulePlanBuilder, RulePlanExecutor
from rule.rule_reader import RuleReader


def get_runner():
    config = Configurator().get_configurator()
    engine = create_engine(config)

    query_runner = QueryRunner(config, engine)

    lag_checker = LagChecker(query_runner)

    metric_name = config.utility.data_lag.metric_name
    if metric_name is None:
        metric_name = 'default_data_lag'
        warnings.warn("Using default metric name: [{}]".format(metric_name))

    label = []
    env_label = config.environment.shared.env

    # extract lable from env vars to add it to metrics
    if env_label is None:
        env_label = 'prod'
    label.append(('env', env_label))

    suppress_prometheus = bool(config.environment.shared.suppress_prometheus == 'True')
    prometheus_manager = PrometheusManager(port=9091,
                                           labels=label,
                                           suppress=suppress_prometheus)

    rules = config.utility.data_lag.rules
    rule_reader = RuleReader(rules)
    rule_plan_builder = RulePlanBuilder()
    rule_plan_executor = RulePlanExecutor()

    return DataLagRunner(config=config,
                         engine=engine,
                         lag_checker=lag_checker,
                         prometheus_manager=prometheus_manager,
                         rule_reader=rule_reader,
                         rule_plan_builder=rule_plan_builder,
                         rule_plan_executor=rule_plan_executor,
                         metric_name=metric_name)


def main():
    runner = get_runner()
    runner.run()


if __name__ == '__main__':
    main()
