import warnings
from typing import List, Dict

from rule.rules import get_registered_rules, get_rule_order
from lag.lag_payload import LagPayload, PayloadColumn


class RulePlan:
    __slots__ = '__rule_chain'

    def __init__(self):
        self.__rule_chain = list()

    def __iter__(self):
        return iter(self.__rule_chain)

    def add_step(self, step):
        self.__rule_chain.append(step)


class RulePlanBuilder:
    __slots__ = ('rule_dict', 'rule_order')

    def __init__(self):
        self.rule_dict = get_registered_rules()
        self.rule_order = get_rule_order()

    def build_plan(self, raw_rules: Dict) -> RulePlan:
        rule_plan = RulePlan()
        for current_rule_name in self.rule_order:
            next_callable = self.__create_next_callable(raw_rules, current_rule_name)
            rule_plan.add_step(next_callable)
        if bool(raw_rules):
            warnings.warn("Some of the user defined rules is not found in the "
                          f"catalog. {raw_rules}")
        return rule_plan

    def __create_next_callable(self, raw_rules, rule_name) -> callable:
        """
        Extracts parameters from the raw_rules and prepares a callable with
        pre-set parameters

        :param raw_rules: external rules
        :param rule_name: name of the rule function
        :return: prepared callable to mutate rule functions last argument or
            identity function if declaration of the rule is not found
        """
        rule_function = self.rule_dict[rule_name]
        bound_parameters = rule_function._bound_params
        function_parameters = list()
        for external_param_name, required in bound_parameters.items():
            try:
                external_param = raw_rules[external_param_name]
                function_parameters.append(external_param)
            except KeyError:
                if required:
                    raise RuntimeError(f"Parameter {external_param_name} is "
                                       f"required.")
            finally:
                raw_rules.pop(external_param_name, None)

        if len(function_parameters) == 0:
            print(f"No parameters found for rule {rule_name}. Skipping...")
            return lambda x: x

        return lambda x: rule_function(*function_parameters, x)


class RulePlanExecutor:

    def apply_rules(self, schema, tables: List[str],
                    rule_plan: RulePlan) -> List[LagPayload]:
        """
        Apply rule_plan to the supplied list of table names

        :param schema: schema related to the supplied tables
        :param tables: list of table names
        :param rule_plan: prepared execution plan
        :return: list of LagPayload wrappers
        """
        tables = [{PayloadColumn.TABLE_NAME: table_name,
                   PayloadColumn.SCHEMA: schema}
                  for table_name in tables]
        filtered_tables = tables[:]

        for rule in rule_plan:
            filtered_tables = rule(filtered_tables)

        payloads = list()
        for table in filtered_tables:
            payloads.append(
                LagPayload(
                    schema=schema,
                    table_name=table[PayloadColumn.TABLE_NAME],
                    lag_check_strategy=table[PayloadColumn.CHECK_STRATEGY],
                    threshold=table[PayloadColumn.THRESHOLD_VALUE],
                    partition_column=table[PayloadColumn.PARTITION_COLUMN]
                )
            )
        return payloads
