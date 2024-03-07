import datetime
from unittest import TestCase
from unittest.mock import MagicMock

from main import WorkExperienceRunner


class TestMergePeriods(TestCase):
    def setUp(self) -> None:
        config = MagicMock()
        we_runner = WorkExperienceRunner(config, None, None, None, None)
        self.merge_periods = we_runner.merge_periods
        self.date_now = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

        self.periods_base = [
            ('Employee', datetime.datetime(2022, 7, 1, 0, 0), datetime.datetime(2022, 9, 30, 0, 0)),
            ('Employee', datetime.datetime(2022, 10, 1, 0, 0), datetime.datetime(2022, 10, 31, 0, 0)),
            ('Intern', datetime.datetime(2022, 11, 1, 0, 0), datetime.datetime(2022, 11, 30, 0, 0)),
            ('Intern', datetime.datetime(2022, 12, 1, 0, 0), self.date_now)
        ]
        self.one_record_periods = [
            ('Employee', datetime.datetime(2022, 7, 1, 0, 0), self.date_now)
        ]

        self.dismissal_records_periods = [
            ('Employee', datetime.datetime(2022, 1, 1, 0, 0), datetime.datetime(2022, 4, 30, 0, 0)),
            ('Dismissal', datetime.datetime(2022, 5, 1, 0, 0),  self.date_now),
        ]

        self.extended_vacation_records_periods = [
            ('Employee', datetime.datetime(2022, 2, 1, 0, 0), datetime.datetime(2022, 4, 30, 0, 0)),
            ('Extended Vacation', datetime.datetime(2022, 5, 1, 0, 0),  self.date_now),
        ]

    def test_merge_period_count(self):
        start_count_date = datetime.datetime(2022, 7, 1, 0, 0)
        actual = self.merge_periods(self.periods_base, start_count_date)
        self.assertEqual(1, len(actual))

    def test_merge_period(self):
        start_count_date = datetime.datetime(2022, 7, 1, 0, 0)
        actual = self.merge_periods(self.periods_base, start_count_date)[0]

        self.assertEqual(start_count_date, actual.get_start_date())
        self.assertEqual(self.date_now, actual.get_end_date())
        self.assertEqual(True, actual.counts_towards_exp)
        self.assertEqual(False, actual.is_dismissal)

    def test_merge_period_one_record_case_count(self):
        start_count_date = datetime.datetime(2022, 7, 1, 0, 0)
        actual = self.merge_periods(self.one_record_periods, start_count_date)
        self.assertEqual(1, len(actual))

    def test_merge_period_one_record_case(self):
        start_count_date = datetime.datetime(2022, 7, 1, 0, 0)
        actual_employee = self.merge_periods(self.one_record_periods, start_count_date)[0]
        self.assertEqual(start_count_date, actual_employee.get_start_date())
        self.assertEqual(self.date_now, actual_employee.get_end_date())

    def test_merge_period_dismissal_record_case_count(self):
        start_count_date = datetime.datetime(2022, 1, 1, 0, 0)
        actual = self.merge_periods(self.dismissal_records_periods, start_count_date)
        self.assertEqual(2, len(actual))

    def test_merge_period_dismissal_record_case(self):
        start_count_date = datetime.datetime(2022, 1, 1, 0, 0)
        actual = self.merge_periods(self.dismissal_records_periods, start_count_date)
        actual_employee = actual[0]
        expect_period_end_employee = datetime.datetime(2022, 4, 30, 0, 0)
        self.assertEqual(start_count_date, actual_employee.get_start_date())
        self.assertEqual(expect_period_end_employee, actual_employee.get_end_date())
        self.assertEqual(True, actual_employee.counts_towards_exp)
        self.assertEqual(False, actual_employee.is_dismissal)

        actual_dismissal = actual[1]
        expect_dismissal_start_date = datetime.datetime(2022, 5, 1, 0, 0)
        expect_period_end_dismissal = self.date_now
        self.assertEqual(expect_dismissal_start_date, actual_dismissal.get_start_date())
        self.assertEqual(expect_period_end_dismissal, actual_dismissal.get_end_date())
        self.assertEqual(False, actual_dismissal.counts_towards_exp)
        self.assertEqual(True, actual_dismissal.is_dismissal)

    def test_merge_period_extended_vacation_record_case(self):
        start_count_date = datetime.datetime(2022, 2, 1, 0, 0)
        actual = self.merge_periods(self.extended_vacation_records_periods, start_count_date)

        actual_employee = actual[0]
        expect_period_end_employee = datetime.datetime(2022, 4, 30, 0, 0)
        self.assertEqual(start_count_date, actual_employee.get_start_date())
        self.assertEqual(expect_period_end_employee, actual_employee.get_end_date())
        self.assertEqual(True, actual_employee.counts_towards_exp)
        self.assertEqual(False, actual_employee.is_dismissal)

        actual_dismissal = actual[1]
        expect_extended_vacation_start_date = datetime.datetime(2022, 5, 1, 0, 0)
        expect_extended_vacation_period_end = self.date_now
        self.assertEqual(expect_extended_vacation_start_date, actual_dismissal.get_start_date())
        self.assertEqual(expect_extended_vacation_period_end, actual_dismissal.get_end_date())
        self.assertEqual(False, actual_dismissal.counts_towards_exp)
        self.assertEqual(False, actual_dismissal.is_dismissal)
