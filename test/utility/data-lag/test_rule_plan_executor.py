from unittest import TestCase
from rule.rule_plan import RulePlanExecutor
from rule.rule_plan import RulePlanBuilder
from rule.rule_reader import RuleReader

from lag.lag_payload import PayloadColumn


class TestRulePlanExecutor(TestCase):
    def setUp(self) -> None:
        raw_rules = """rules:
          - dbo:
              default-partition-column: a
              default-threshold-value: 1d
              default-check-strategy: batch
          """
        self.base_raw_rules = RuleReader(raw_rules).get_schema_rule_plan('dbo')
        self.plan_builder = RulePlanBuilder()
        self.plan_executor = RulePlanExecutor()

    def __extract_specified_key(self, key, data):
        return [getattr(item, key) for item in data]

    def __append_rule_to_raw_rules_and_exec(self,
                                            rule_key,
                                            rule_value,
                                            tables):
        self.base_raw_rules[rule_key] = rule_value
        exec_plan = self.plan_builder.build_plan(self.base_raw_rules)
        return self.plan_executor.apply_rules('dbo', tables, exec_plan)

    def test_only_with_prefix_dim_should_survive(self):
        tables = ['dim_some_table',
                  'dim_another_table',
                  'funny_table_name',
                  'not_funny_table_name']

        out = self.__append_rule_to_raw_rules_and_exec(
            'allowed-prefixes',
            'dim',
            tables
        )

        actual = self.__extract_specified_key(
            PayloadColumn.TABLE_NAME,
            out)

        expected = [table for table in tables if table.startswith('dim')]

        self.assertEqual(expected, actual)

    def test_only_with_postfix_latest_should_survive(self):
        tables = [
            'some_table',
            'this_is_latest',
            'latest',
            'another'
        ]

        out = self.__append_rule_to_raw_rules_and_exec(
            'allowed-postfixes',
            'latest',
            tables
        )

        actual = self.__extract_specified_key(
            PayloadColumn.TABLE_NAME,
            out)
        expected = [table for table in tables if table.endswith('latest')]

        self.assertEqual(expected, actual)

    def test_remove_tables_with_forbidden_prefix(self):
        tables = [
            'forbidden',
            'forbidden_table',
            'wow_table'
        ]

        out = self.__append_rule_to_raw_rules_and_exec(
            'forbidden-prefixes',
            'forbidden',
            tables
        )

        actual = self.__extract_specified_key(
            PayloadColumn.TABLE_NAME,
            out
        )
        expected = [table for table in tables
                    if not table.startswith('forbidden')]

        self.assertEqual(expected, actual)

    def test_remove_tables_with_forbidden_postfixes(self):
        tables = [
            'keep_me',
            'this_is_forbidden',
        ]

        out = self.__append_rule_to_raw_rules_and_exec(
            'forbidden-postfixes',
            ('forbidden',),
            tables
        )

        actual = self.__extract_specified_key(
            PayloadColumn.TABLE_NAME,
            out
        )
        expected = [table for table in tables
                    if not table.endswith('forbidden')]

        self.assertEqual(expected, actual)

    def test_remove_ignored_tables(self):
        tables = [
            'dim_employees',
            'other_table',
            'ignore_me'
        ]

        out = self.__append_rule_to_raw_rules_and_exec(
            'ignore-tables',
            ('employees', 'ignore'),
            tables
        )
        actual = self.__extract_specified_key(
            PayloadColumn.TABLE_NAME,
            out
        )

        expected = list()
        for table in tables:
            if not any(x in table for x in ('employees', 'ignore')):
                expected.append(table)

        self.assertEqual(expected, actual)

    def __generic_overrides_proc(self,
                                 tables,
                                 new_rule_key,
                                 new_rule_value,
                                 default_rule_key,
                                 table_substring,
                                 expected_affected_table_name,
                                 key_to_check):
        self.base_raw_rules[new_rule_key] = [{table_substring: new_rule_value}]
        default_value = self.base_raw_rules[default_rule_key]
        exec_plan = self.plan_builder.build_plan(self.base_raw_rules)
        out = self.plan_executor.apply_rules('dbo', tables, exec_plan)

        for item in out:
            if item.table_name == expected_affected_table_name:
                self.assertEqual(getattr(item, key_to_check), new_rule_value)
            else:
                self.assertEqual(getattr(item, key_to_check), default_value)

    def test_partition_column_overrides(self):
        tables = [
            'overriden_partition',
            'overriden_threshold',
            'overriden_check_strategy',
            'default'
        ]

        self.__generic_overrides_proc(
            tables,
            'partition-column-overrides',
            'another_partition_column',
            'default-partition-column',
            'partition',
            'overriden_partition',
            PayloadColumn.PARTITION_COLUMN
        )

    def test_threshold_values_overrides(self):
        tables = [
            'overriden_partition',
            'overriden_threshold',
            'overriden_check_strategy',
            'default'
        ]

        self.__generic_overrides_proc(
            tables,
            'threshold-values-overrides',
            'another_threshold_value',
            'default-threshold-value',
            'threshold',
            'overriden_threshold',
            PayloadColumn.THRESHOLD_VALUE
        )

    def test_check_strategy_overrides(self):
        tables = [
            'overriden_partition',
            'overriden_threshold',
            'overriden_check_strategy',
            'default'
        ]

        self.__generic_overrides_proc(
            tables,
            'check-strategy-overrides',
            'another_check_strategy',
            'default-check-strategy',
            'strategy',
            'overriden_check_strategy',
            PayloadColumn.CHECK_STRATEGY
        )
