from unittest import TestCase

from rule.rule_reader import RuleReader
from rule.rule_plan import RulePlanBuilder


class TestRulePlanBuilder(TestCase):
    def setUp(self) -> None:
        raw_rules = """rules:
  - without-required-parameter:
      default-threshold-value: 1d
      default-check-strategy: batch
  """
        self.rule_reader = RuleReader(raw_rules)
        self.rule_plan_builder = RulePlanBuilder()

    def test_throw_exception_when_no_required_parameter(self):
        raw_plan = self.rule_reader.get_schema_rule_plan('without-required-parameter')

        self.assertRaises(RuntimeError,
                          self.rule_plan_builder.build_plan,
                          raw_plan)
