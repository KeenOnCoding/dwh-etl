import datetime
from dateutil import relativedelta
from unittest import TestCase
from unittest.mock import MagicMock

from db.query_runner import QueryRunner
from time_interval import TimeInterval


class TestTimeInterval(TestCase):
    def setUp(self):
        self.query_runner = None
        self.source_table = None
        self.target_table = None

        self.time = TimeInterval()
        self.get_interval_to_fetch = self.time.get_interval_to_fetch
        self.get_max_date = self.time.get_max_date
        self.get_ranges = self.time.get_ranges

        self.query_runner = QueryRunner(None, None)
        self.default_date = datetime.datetime(2003, 1, 2, 0, 0)
        self.latest_date = datetime.datetime(2021, 12, 31, 0, 0)

    def test_get_max_date(self):
        # Target table has data
        self.query_runner.query_single_result = MagicMock(return_value=self.latest_date)
        actual = self.get_max_date(self.query_runner, self.source_table, self.target_table)
        actual_date, actual_status = actual
        self.assertEqual(self.latest_date, actual_date)
        self.assertEqual(False, actual_status)

    def test_get_max_date_empty_target_case(self):
        # Target table is empty
        self.query_runner.query_single_result = MagicMock(side_effect=[None, self.default_date])
        actual = self.get_max_date(self.query_runner, self.source_table, self.target_table)
        actual_date, actual_status = actual
        self.assertEqual(self.default_date, actual_date,)
        self.assertEqual(True, actual_status)

    def test_get_max_date_empty_case(self):
        # Target and source tables are empty
        self.query_runner.query_single_result = MagicMock(side_effect=[None, None])
        with self.assertRaises(ValueError) as context:
            self.get_max_date(self.query_runner, self.source_table, self.target_table)
            self.assertTrue("Source table have no min date. " in context.exception)

    def test_get_interval_to_fetch(self):
        # Case when target table has something records, and next have date not more than now
        self.time.get_max_date = MagicMock(return_value=(self.latest_date, False))

        actual_start_date, actual_end_date = self.get_interval_to_fetch(None, self.source_table, self.target_table)

        expected_start_date = self.latest_date + relativedelta.relativedelta(months=1)
        expected_end_date = expected_start_date + relativedelta.relativedelta(months=6)

        self.assertEqual(expected_start_date, actual_start_date)
        self.assertEqual(expected_end_date, actual_end_date)

    def test_get_interval_to_fetch_default_date_case(self):
        # Case when target table is empty, and as latest date was chosen minimal available date from source table
        self.time.get_max_date = MagicMock(return_value=(self.default_date, True))

        actual_start_date, actual_end_date = self.get_interval_to_fetch(None, self.source_table, self.target_table)

        expected_start_date = self.default_date
        expected_end_date = expected_start_date + relativedelta.relativedelta(months=6)

        self.assertEqual(expected_start_date, actual_start_date)
        self.assertEqual(expected_end_date, actual_end_date)

    def test_get_interval_to_fetch_current_month_case(self):
        # Case when target table have data for current month or in range (end_of_current_month: current_month - 6)
        date_now = datetime.datetime.today() + relativedelta.relativedelta(day=31, hour=0, minute=0, second=0)
        self.time.get_max_date = MagicMock(return_value=(date_now, False))

        actual_start_date, actual_end_date = self.get_interval_to_fetch(None, self.source_table, self.target_table)

        expected_start_date = date_now.date() - relativedelta.relativedelta(months=6)
        expected_end_date = actual_end_date.date()

        self.assertEqual(expected_start_date, actual_start_date.date())
        self.assertEqual(expected_end_date, actual_end_date.date())

    def test_get_range(self):
        start_date = datetime.datetime(2022, 6, 30, 0, 0, 0)
        end_date = datetime.datetime(2022, 12, 31, 0, 0, 0)
        actual = self.get_ranges(start_date, end_date)
        month_difference = end_date.month - start_date.month
        self.assertEqual(month_difference, len(actual))
