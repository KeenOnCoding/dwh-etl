import datetime
from unittest import TestCase
from unittest.mock import MagicMock

from processor.work_experience_processor.working_period import WorkingPeriod
from processor.work_experience_processor.main import WorkExperienceRunner


class TestCountByDate(TestCase):
    def setUp(self):
        config = MagicMock()
        we_runner = WorkExperienceRunner(config, None, None, None, None)
        self.count_by_period = we_runner.count_day_by_period
        self.datetime_now = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

    def test_count_by_date_intern_employee(self):
        start_count_date = datetime.datetime(2020, 11, 19, 0, 0, 0)

        periods = [
            WorkingPeriod(datetime.datetime(2020, 10, 19, 0, 0, 0), datetime.datetime(2020, 10, 18, 0, 0, 0),
                          counts_towards_exp=False, is_dismissal=False),
            WorkingPeriod(datetime.datetime(2020, 11, 19, 0, 0, ), self.datetime_now,
                          counts_towards_exp=True, is_dismissal=False)
        ]

        # Person become employee
        actual = self.count_by_period(periods, datetime.datetime(2020, 12, 19, 0, 0, 0), start_count_date)
        self.assertEqual(1, int(actual))

        # Person become employee, but does not have 1 full mont experience
        actual = int(self.count_by_period(periods, datetime.datetime(2020, 12, 17, 0, 0, 0), start_count_date))
        self.assertEqual(0, actual)

        # Experience before internship
        actual = int(self.count_by_period(periods, datetime.datetime(2020, 9, 17, 0, 0, 0), start_count_date))
        self.assertEqual(-1, actual)

        # Experience before start count date, but after start internship
        actual = int(self.count_by_period(periods, datetime.datetime(2020, 11, 10, 0, 0, 0), start_count_date))
        self.assertEqual(-1, actual)

    def test_count_by_date_int_dism(self):
        start_count_date = datetime.datetime.now()

        periods = [
            WorkingPeriod(datetime.datetime(2020, 10, 19, 0, 0, 0), datetime.datetime(2020, 11, 18, 0, 0, 0),
                          counts_towards_exp=False, is_dismissal=False),
            WorkingPeriod(datetime.datetime(2020, 11, 19, 0, 0, 0), self.datetime_now,
                          counts_towards_exp=False, is_dismissal=False)
        ]

        # Experience after dismissal
        actual = self.count_by_period(periods, datetime.datetime(2020, 12, 19, 0, 0, 0), start_count_date)
        self.assertEqual(-1, actual)

        # Experience during internship
        actual = self.count_by_period(periods, datetime.datetime(2020, 11, 1, 0, 0, 0), start_count_date)
        self.assertEqual(-1, actual)

    def test_count_by_date_employee_dismissal(self):
        start_count_date = datetime.datetime(2020, 10, 19, 0, 0, 0)

        periods = [
            WorkingPeriod(datetime.datetime(2020, 10, 19, 0, 0, 0), datetime.datetime(2020, 11, 29, 0, 0, 0),
                          counts_towards_exp=True, is_dismissal=False),
            WorkingPeriod(datetime.datetime(2020, 11, 29, 0, 0, 0), self.datetime_now,
                          counts_towards_exp=False, is_dismissal=True)
        ]

        # Person have 1+ month exp
        actual = self.count_by_period(periods, datetime.datetime(2020, 11, 25, 0, 0, 0), start_count_date)
        self.assertEqual(1, int(actual))

        # Experience next month after dismissal
        actual = self.count_by_period(periods, datetime.datetime(2020, 12, 1, 0, 0, 0), start_count_date)
        self.assertEqual(-1, actual)

        # Experience at the end of the month of dismissal
        actual = self.count_by_period(periods, datetime.datetime(2020, 11, 30, 0, 0, 0), start_count_date)
        self.assertEqual(1, int(actual))

    def test_count_by_date_emp_vac_emp(self):
        periods = [
            WorkingPeriod(datetime.datetime(2020, 1, 19, 0, 0, 0), datetime.datetime(2020, 2, 18, 0, 0, 0), True, False),
            WorkingPeriod(datetime.datetime(2020, 2, 18, 0, 0, 0), datetime.datetime(2020, 4, 19, 0, 0, 0), False, False),
            WorkingPeriod(datetime.datetime(2020, 4, 19, 0, 0, 0), datetime.datetime(2020, 5, 19, 0, 0, 0), True, False),
            WorkingPeriod(datetime.datetime(2020, 5, 19, 0, 0, 0), datetime.datetime(2020, 7, 19, 0, 0, 0), False, False),
            WorkingPeriod(datetime.datetime(2020, 7, 19, 0, 0, 0), self.datetime_now, True, False)
        ]

        start_count_date = datetime.datetime(2020, 1, 19, 0, 0, 0)

        # Experience before start date
        actual = self.count_by_period(periods, datetime.datetime(2020, 1, 1, 0, 0, 0), start_count_date)
        self.assertEqual(-1, actual)

        # Experience by first extended vacation date
        actual = self.count_by_period(periods, datetime.datetime(2020, 2, 18, 0, 0, 0), start_count_date)
        self.assertEqual(1, int(actual))

        # Experience in vacation
        actual = self.count_by_period(periods, datetime.datetime(2020, 3, 18, 0, 0, 0), start_count_date)
        self.assertEqual(1, int(actual))

        # Experience in second vacation
        actual = self.count_by_period(periods, datetime.datetime(2020, 6, 25, 0, 0, 0), start_count_date)
        self.assertEqual(2, int(actual))

        # Experience after second vacation at first working date
        actual = self.count_by_period(periods, datetime.datetime(2020, 7, 25, 0, 0, 0), start_count_date)
        self.assertEqual(2, int(actual))

    def test_count_by_date_emp_vac_dism(self):
        start_count_date = datetime.datetime(2020, 1, 19, 0, 0, 0)

        period = [
            WorkingPeriod(datetime.datetime(2020, 1, 19, 0, 0, 0), datetime.datetime(2020, 2, 18, 0, 0, 0), True, False),
            WorkingPeriod(datetime.datetime(2020, 2, 18, 0, 0, 0), datetime.datetime(2020, 4, 19, 0, 0, 0),False, False),
            WorkingPeriod(datetime.datetime(2020, 4, 19, 0, 0, 0), self.datetime_now, False, True)
        ]

        # Experience before start date
        actual = self.count_by_period(period, datetime.datetime(2020, 1, 1, 0, 0, 0), start_count_date)
        self.assertEqual(-1, actual)

        # Experience before start date
        actual = self.count_by_period(period, datetime.datetime(2020, 1, 1, 0, 0, 0), start_count_date)
        self.assertEqual(-1, actual)

        # Experience by first extended vacation date
        actual = self.count_by_period(period, datetime.datetime(2020, 2, 18, 0, 0, 0), start_count_date)
        self.assertEqual(1, int(actual))

        # Experience by end extended vacation date
        actual = self.count_by_period(period, datetime.datetime(2020, 4, 19, 0, 0, 0), start_count_date)
        self.assertEqual(1, int(actual))

        # Experience at the end of the month of dismissal
        actual = self.count_by_period(period, datetime.datetime(2020, 4, 30, 0, 0, 0), start_count_date)
        self.assertEqual(1, int(actual))

    def test_count_by_date_empl_dism_empl_dism(self):
        start_count_date = datetime.datetime(2016, 1, 19, 0, 0, 0)

        period = [
            WorkingPeriod(datetime.datetime(2020, 1, 19, 0, 0, 0), datetime.datetime(2020, 2, 18, 0, 0, 0), True, False),
            WorkingPeriod(datetime.datetime(2020, 2, 18, 0, 0, 0), datetime.datetime(2020, 4, 19, 0, 0, 0), False, True),
            WorkingPeriod(datetime.datetime(2020, 4, 19, 0, 0, 0), datetime.datetime(2020, 5, 19, 0, 0, 0), True, False),
            WorkingPeriod(datetime.datetime(2020, 5, 19, 0, 0, 0), self.datetime_now, False, True),
        ]

        # Experience before start date
        actual = self.count_by_period(period, datetime.datetime(2020, 1, 1, 0, 0, 0), start_count_date)
        self.assertEqual(-1, actual)

        # Experience by last date of work
        actual = self.count_by_period(period, datetime.datetime(2020, 2, 18, 0, 0, 0), start_count_date)
        self.assertEqual(1, int(actual))

        # Experience at the end of the month of dismissal
        actual = self.count_by_period(period, datetime.datetime(2020, 2, 29, 0, 0, 0), start_count_date)
        self.assertEqual(1, int(actual))

        # Experience in status dismissal
        actual = self.count_by_period(period, datetime.datetime(2020, 4, 1, 0, 0, 0), start_count_date)
        self.assertEqual(-1, int(actual))

        # Experience in first day after returning
        actual = self.count_by_period(period, datetime.datetime(2020, 4, 19, 0, 0, 0), start_count_date)
        self.assertEqual(0, int(actual))

        # Experience at re-dismissal moths
        actual = self.count_by_period(period, datetime.datetime(2020, 5, 19, 0, 0, 0), start_count_date)
        self.assertEqual(1, int(actual))

        # Experience after re-dismissal without returning
        actual = self.count_by_period(period, datetime.datetime(2020, 6, 1, 0, 0, 0), start_count_date)
        self.assertEqual(-1, int(actual))

    def test_count_by_date_emp_int_emp(self):
        start_count_date = datetime.datetime(2020, 1, 19, 0, 0, 0)

        period = [
            WorkingPeriod(datetime.datetime(2020, 1, 19, 0, 0, 0), datetime.datetime(2020, 2, 18, 0, 0, 0), True, False),
            WorkingPeriod(datetime.datetime(2020, 2, 19, 0, 0, 0), datetime.datetime(2020, 4, 18, 0, 0, 0), True, False),
            WorkingPeriod(datetime.datetime(2020, 4, 19, 0, 0, 0), datetime.datetime(2020, 5, 18, 0, 0, 0), True, False),
        ]

        # Experience before start date
        actual = self.count_by_period(period, datetime.datetime(2020, 1, 1, 0, 0, 0), start_count_date)
        self.assertEqual(-1, actual)

        actual = self.count_by_period(period, datetime.datetime(2020, 2, 19, 0, 0, 0), start_count_date)
        self.assertEqual(1, int(actual))

        actual = self.count_by_period(period, datetime.datetime(2020, 3, 19, 0, 0, 0), start_count_date)
        self.assertEqual(2, int(actual))

        actual = self.count_by_period(period, datetime.datetime(2020, 5, 19, 0, 0, 0), start_count_date)
        self.assertEqual(4, int(actual))
