import re
from unittest import TestCase

from lag.lag_payload import PayloadColumn
from rule.rules import rule_add_default_check_strategy
from rule.rules import rule_add_default_partition_column
from rule.rules import rule_add_default_threshold_value
from rule.rules import rule_keep_this_postfixes, rule_discard_this_postfixes
from rule.rules import rule_keep_this_prefixes, rule_discard_this_prefixes
from rule.rules import rule_override_check_strategy
from rule.rules import rule_override_partition_column
from rule.rules import rule_override_threshold_value
from rule.rules import rule_remove_ignored_tables


class Rules(TestCase):
    def setUp(self) -> None:
        table_names = [
            "dim_some_table",
            "fact_another_table",
            "temp_wow",
            "some",
            "some_table",
            "overriden_some"
        ]
        self.tables = [dict(**{PayloadColumn.TABLE_NAME
                               : table}) for table in table_names]

    def __add_value_to_list_of_dicts(self, key, value):
        return [dict(item, **{key: value})
                for item in self.tables]

    def test_add_default_partition_column(self):
        partition_column_value = "batch_id"

        actual = \
            rule_add_default_partition_column(partition_column_value,
                                              self.tables)
        expected = self.__add_value_to_list_of_dicts(
            PayloadColumn.PARTITION_COLUMN,
            partition_column_value)

        self.assertEqual(expected, actual)

    def test_add_default_threshold_value(self):
        threshold_value = "4d"

        actual = \
            rule_add_default_threshold_value(threshold_value, self.tables)
        expected = self.__add_value_to_list_of_dicts(
            PayloadColumn.THRESHOLD_VALUE,
            threshold_value
        )
        self.assertEqual(expected, actual)

    def test_add_default_check_strategy(self):
        default_check_strategy = "batch_id"
        actual = rule_add_default_check_strategy(default_check_strategy,
                                                 self.tables)
        expected = self.__add_value_to_list_of_dicts(
            PayloadColumn.CHECK_STRATEGY,
            default_check_strategy
        )
        self.assertEqual(expected, actual)

    def test_filter_by_table_prefix_keep(self):
        keep_prefixes = ("dim", "fact")
        actual = rule_keep_this_prefixes(keep_prefixes, self.tables)
        expected = [item for item in self.tables
                    if item["table_name"].startswith(keep_prefixes)]

        self.assertEqual(expected, actual)

    def test_filter_by_table_prefix_discard(self):
        discard_prefixes = ("dim",)
        actual = rule_discard_this_prefixes(discard_prefixes, self.tables)
        expected = [item for item in self.tables
                    if not item["table_name"].startswith(discard_prefixes)]

        self.assertEqual(expected, actual)

    def test_filter_by_postfix_keep(self):
        keep_postfixes = ('wow',)
        actual = rule_keep_this_postfixes(keep_postfixes, self.tables)
        expected = [item for item in self.tables
                    if item["table_name"].endswith(keep_postfixes)]

        self.assertEqual(expected, actual)

    def test_filter_by_postfix_discard(self):
        discard_postfixes = ('table', 'wow')
        actual = rule_discard_this_postfixes(discard_postfixes, self.tables)
        expected = [item for item in self.tables
                    if not item["table_name"].endswith(discard_postfixes)]

        self.assertEqual(expected, actual)

    def test_filter_by_ignored_tables(self):
        ignore_tables = ('some', 'another')
        actual = rule_remove_ignored_tables(ignore_tables, self.tables)
        pattern = re.compile("|".join(ignore_tables))
        expected = [item for item in self.tables
                    if not pattern.search(item["table_name"])]

        self.assertEqual(expected, actual)

    def __prepare_overriden_list(self, key, list_table_to_value):
        result = list()

        table_to_value = dict()
        for dictionary in list_table_to_value:
            table_to_value.update(dictionary)

        for item in self.tables:
            curr_table_name = item[PayloadColumn.TABLE_NAME]

            substitute_value_key = None
            for table in list(table_to_value):
                if table in curr_table_name:
                    substitute_value_key = table
                    break

            if substitute_value_key is not None:
                item[key] = table_to_value[substitute_value_key]
            result.append(item)

        return result

    def test_override_partition_column_without_applying_default_value(self):
        table_name = "dim_some_table"
        override_partition_column = [{table_name: "batch_id"}]
        actual = rule_override_partition_column(override_partition_column,
                                                self.tables)
        expected = self.__prepare_overriden_list(
            PayloadColumn.PARTITION_COLUMN,
            override_partition_column
        )

        self.assertEqual(expected, actual)

    def test_override_threshold_value(self):
        table_name = "some"
        override_threshold_column = [{table_name: "25d"}]
        actual = rule_override_threshold_value(
            override_threshold_column,
            self.tables
        )
        expected = self.__prepare_overriden_list(
            PayloadColumn.THRESHOLD_VALUE,
            override_threshold_column
        )
        self.assertEqual(expected, actual)

    def test_override_check_strategy(self):
        table_name = "dim_some_table"
        override_threshold_column = [{table_name: "date2"}]
        actual = rule_override_check_strategy(
            override_threshold_column,
            self.tables
        )
        expected = self.__prepare_overriden_list(
            PayloadColumn.CHECK_STRATEGY,
            override_threshold_column
        )

        self.assertEqual(expected, actual)
