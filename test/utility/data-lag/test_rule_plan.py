from unittest import TestCase
from rule.rule_plan import RulePlan


class TestRulePlan(TestCase):
    def test_should_iterate_N_times(self):
        rule_plan = RulePlan()
        expected_iterations = 5
        for _ in range(expected_iterations):
            rule_plan.add_step("")

        actual_iterations = 0
        for _ in rule_plan:
            actual_iterations += 1

        self.assertEqual(expected_iterations, actual_iterations)
