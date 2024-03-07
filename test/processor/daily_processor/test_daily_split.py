import datetime
import unittest
from unittest import TestCase
from unittest.mock import MagicMock

from processor.daily_processor.main import split_to_daily


class TestDailySplit(TestCase):

    def setUp(self):
        self.daily_split = split_to_daily
        self.attrs = {'get_input_fields.return_value':
                          ['batch_id', 'id', 'employee_id', 'contractor', 'part_time',
                           'is_project_internal', 'linear_manager_id', 'project_id',
                           'task', 'issue_id', 'approval_tag_group', 'approval_tag',
                           'effort', 'overtime', 'non_billable_comment', 'status',
                           'billability', 'started', 'finished', 'modification_date',
                           'category_id', 'department_id', 'position_id']}
        mock = MagicMock(**self.attrs)

        def set_output_context(context):
            mock.get_input_fields.return_value = context

        mock.set_output_fields = set_output_context
        self.mock = mock

    def tearDown(self):
        self.daily_split = None
        self.mock = None

    def test_daily_split(self):
        # Arrange
        # Tue,29, 2020, Month #12
        started = datetime.datetime(2020, 12, 29, 0, 0, 0)
        # Fri,1, 2021, Month #1
        finished = datetime.datetime(2020, 12, 30, 0, 0, 0)
        overtime = False

        # Act
        actual = self.daily_split(started, finished, overtime, [])

        # Assert
        exp = [(datetime.datetime(2020, 12, 29, 0, 0, 0), datetime.datetime(2020, 12, 29, 0, 0, 0)),
               (datetime.datetime(2020, 12, 30, 0, 0, 0), datetime.datetime(2020, 12, 30, 0, 0, 0))]
        self.assertEqual(exp, actual)

    def test_one_day_daily_split(self):
        # Arrange
        # Tue,29, 2020, Month #12
        started = datetime.datetime(2020, 12, 29, 0, 0, 0)
        # Tue,29, 2020, Month #12
        finished = datetime.datetime(2020, 12, 29, 0, 0, 0)
        overtime = False

        # Act
        actual = self.daily_split(started, finished, overtime, [])

        # Assert
        exp = [(datetime.datetime(2020, 12, 29, 0, 0, 0), datetime.datetime(2020, 12, 29, 0, 0, 0))]
        self.assertEqual(exp, actual)

    def test_weekend_daily_split(self):
        # Arrange
        # Sun,27, 2020, Month #12
        started = datetime.datetime(2020, 12, 27, 0, 0, 0)
        # Fri,1, 2021, Month #1
        finished = datetime.datetime(2021, 1, 1, 0, 0, 0)
        overtime = False

        # Act
        dates = [(datetime.datetime(2020, 12, 27, 0, 0, 0), datetime.datetime(2021, 1, 1, 0, 0, 0))]

        # Assert
        exp = self.daily_split(started, finished, overtime, [])
        self.assertEqual(exp, dates)

    def test_overtime_true_daily_split(self):
        # Arrange
        # Tue,29, 2020, Month #12
        started = datetime.datetime(2020, 12, 29, 0, 0, 0)
        # Tue,29, 2020, Month #12
        finished = datetime.datetime(2021, 1, 1, 0, 0, 0)
        overtime = True

        # Act
        actual = self.daily_split(started, finished, overtime, [])

        # Assert
        exp = [(datetime.datetime(2020, 12, 29, 0, 0, 0), datetime.datetime(2021, 1, 1, 0, 0, 0))]

        self.assertEqual(exp, actual)


if __name__ == '__main__':
    unittest.main()
