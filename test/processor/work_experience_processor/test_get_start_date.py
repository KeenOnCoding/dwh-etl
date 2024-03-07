import datetime
from unittest import TestCase
from unittest.mock import MagicMock

from processor.work_experience_processor.main import WorkExperienceRunner


class TestGetStartDate(TestCase):
    def setUp(self):
        config = MagicMock()
        we_runner = WorkExperienceRunner(config, None, None, None, None)
        self.get_start_date = we_runner.get_start_date
        self.datetime_now = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

    def test_get_start_date(self):
        employee_history = [
            {
                "id": "b4b96bb2-c6d7-11e6-a911-005056832377",
                "status": "Employee",
                "date": datetime.datetime(2020, 7, 1, 0, 0)
            },
            {
                "id": "b4b96bb2-c6d7-11e6-a911-005056832377",
                "status": "Intern",
                "date": datetime.datetime(2020, 11, 1, 0, 0)
            }
        ]

        actual = self.get_start_date(employee_history)
        expect = datetime.datetime(2020, 7, 1, 0, 0)
        self.assertEqual(expect, actual)

    def test_get_start_date_intern_start(self):
        employee_history = [
            {
                "id": "b4b96bb2-c6d7-11e6-a911-005056832377",
                "status": "Intern",
                "date": datetime.datetime(2022, 1, 1, 0, 0)
            },
            {
                "id": "b4b96bb2-c6d7-11e6-a911-005056832377",
                "status": "Employee",
                "date": datetime.datetime(2022, 7, 1, 0, 0)
            },
        ]

        actual = self.get_start_date(employee_history)
        expect = datetime.datetime(2022, 7, 1, 0, 0)
        self.assertEqual(expect, actual)

    def test_get_start_date_only_intern_status(self):
        employee_history = [
            {
                "id": "b4b96bb2-c6d7-11e6-a911-005056832377",
                "status": "Intern",
                "date": datetime.datetime(2022, 1, 1, 0, 0)
            },
            {
                "id": "b4b96bb2-c6d7-11e6-a911-005056832377",
                "status": "Intern",
                "date": datetime.datetime(2022, 7, 1, 0, 0)
            },
        ]
        actual = self.get_start_date(employee_history)
        expect = self.datetime_now
        self.assertEqual(expect, actual)

    def test_get_start_date_only_dismissal_status(self):
        employee_history = [
            {
                "id": "b4b96bb2-c6d7-11e6-a911-005056832377",
                "status": "Dismissal",
                "date": datetime.datetime(2022, 1, 1, 0, 0)
            }
        ]
        actual = self.get_start_date(employee_history)
        expect = self.datetime_now
        self.assertEqual(expect, actual)

    def test_get_start_date_only_vacation_status(self):
        employee_history = [
            {
                "id": "b4b96bb2-c6d7-11e6-a911-005056832377",
                "status": "Extended vacation",
                "date": datetime.datetime(2022, 1, 1, 0, 0)
            }
        ]
        actual = self.get_start_date(employee_history)
        expect = self.datetime_now
        self.assertEqual(expect, actual)
