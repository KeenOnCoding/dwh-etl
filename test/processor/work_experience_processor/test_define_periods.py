import datetime
from unittest import TestCase
from unittest.mock import MagicMock

from processor.work_experience_processor.main import WorkExperienceRunner


class TestDefinePeriod(TestCase):
    def setUp(self) -> None:
        config = MagicMock()
        we_runner = WorkExperienceRunner(config, None, None, None, None)
        self.define_period = we_runner.define_periods

        self.datetime_now = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

        self.employee_history = [
            {
                "id": "b4b96bb2-c6d7-11e6-a911-005056832377",
                "status": "Employee",
                "date": datetime.datetime(2020, 7, 1, 0, 0)
            },
            {
                "id": "b4b96bb2-c6d7-11e6-a911-005056832377",
                "status": "Employee",
                "date": datetime.datetime(2020, 10, 1, 0, 0)
            },
            {
                "id": "b4b96bb2-c6d7-11e6-a911-005056832377",
                "status": "Intern",
                "date": datetime.datetime(2020, 11, 1, 0, 0)
            }
        ]

        self.employee_history_one_record = [
            {
                "id": "b4b96bb2-c6d7-11e6-a911-005056832377",
                "status": "Employee",
                "date": datetime.datetime(2020, 7, 1, 0, 0)
            }
        ]

    def test_period_count(self):
        actual = self.define_period(self.employee_history)
        self.assertEqual(len(self.employee_history), len(actual))

    def test_define_period(self):
        actual = self.define_period(self.employee_history)

        employee_period_1, employee_period_2, intern_period = actual

        expect_employee_period_1_start = employee_period_1[1]
        expect_employee_periods_1_end = employee_period_1[2]

        expect_employee_period_2_start = employee_period_2[1]
        expect_employee_periods_2_end = employee_period_2[2]

        expect_intern_period_start = intern_period[1]
        expect_intern_period_end = intern_period[2]

        self.assertEqual(expect_employee_period_1_start, datetime.datetime(2020, 7, 1, 0, 0))
        self.assertEqual(expect_employee_periods_1_end, datetime.datetime(2020, 9, 30, 0, 0))

        self.assertEqual(expect_employee_period_2_start, datetime.datetime(2020, 10, 1, 0, 0))
        self.assertEqual(expect_employee_periods_2_end, datetime.datetime(2020, 10, 31, 0, 0))

        self.assertEqual(expect_intern_period_start, datetime.datetime(2020, 11, 1, 0, 0))
        self.assertEqual(expect_intern_period_end, self.datetime_now)

    def test_define_period_one_record_count(self):
        actual = self.define_period(self.employee_history_one_record)[0]

        actual_start = actual[1]
        actual_end = actual[2]

        self.assertEqual(datetime.datetime(2020, 7, 1, 0, 0), actual_start)
        self.assertEqual(self.datetime_now, actual_end)

    def test_define_period_one_record(self):
        actual = self.define_period(self.employee_history_one_record)
        self.assertEqual(len(self.employee_history_one_record), len(actual))
