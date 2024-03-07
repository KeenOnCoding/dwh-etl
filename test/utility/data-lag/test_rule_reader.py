from unittest import TestCase

from rule.rule_reader import RuleReader


class TestRuleReader(TestCase):

    def test_empty_yaml(self):
        self.assertRaises(ValueError, RuleReader, "")

    def test_no_root_object(self):
        rules_without_expected_root = "{some-key: some-value}"
        self.assertRaises(RuntimeError, RuleReader,
                          rules_without_expected_root)

    def test_several_schemas_in_item(self):
        raw_rules = "{rules: [{dbo: {}, dbo2: {}}]}"
        self.assertRaises(RuntimeError, RuleReader, raw_rules)

    def test_get_schemas(self):
        raw_rules = "{rules: [{schema1: {}}, {schema2:{}}]}"
        rule_reader = RuleReader(raw_rules)
        actual_schemas = rule_reader.get_schemas()
        expected_schemas = ["schema1", "schema2"]
        self.assertEqual(actual_schemas, expected_schemas)

    def test_get_schema_rules_with_unknown_schema(self):
        raw_rules = "{rules: [{schema: {}}]}"
        rule_reader = RuleReader(raw_rules)
        self.assertRaises(ValueError, rule_reader.get_schema_rule_plan,
                          "some-other-schema")

    def test_get_raw_schema_rules(self):
        raw_rules = "{rules: [{schema: {rule1: value, rule2: value}}]}"
        rule_reader = RuleReader(raw_rules)
        actual_rule_plan = rule_reader.get_schema_rule_plan("schema")
        expected_rule_plan = {'rule1': 'value', 'rule2': 'value'}

        self.assertEqual(expected_rule_plan, actual_rule_plan)
