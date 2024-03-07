import datetime
from unittest import TestCase
from unittest.mock import MagicMock

from main import WorkExperienceRunner


class TestObjectMap(TestCase):
    def setUp(self) -> None:
        config = MagicMock()
        we_runner = WorkExperienceRunner(config, None, None, None, None)
        self.obj_mapper = we_runner.obj_mapper

        self.data_before_group = [
            {
                "employee_id": "b4b96bb2-c6d7-11e6-a911-005056832377",
                "status": 'Intern',
                "date": datetime.datetime(2022, 1, 1, 0, 0)
            },
            {
                "employee_id": "2af2de89-6963-11e8-a956-005056833d7d",
                "status": 'Employee',
                "date": datetime.datetime(2022, 1, 1, 0, 0)
            },
            {
                "employee_id": "b4b96bb2-c6d7-11e6-a911-005056832377",
                "status": 'Employee',
                "date": datetime.datetime(2022, 8, 1, 0, 0)
            },
            {
                "employee_id": "2af2de89-6963-11e8-a956-005056833d7d",
                "status": 'Employee',
                "date": datetime.datetime(2022, 8, 1, 0, 0)
            },
            {
                "employee_id": "b4b96bb2-c6d7-11e6-a911-005056832377",
                "status": 'Extended vacation',
                "date": datetime.datetime(2022, 9, 1, 0, 0)
            },
            {
                "employee_id": "2af2de89-6963-11e8-a956-005056833d7d",
                "status": 'Employee',
                "date": datetime.datetime(2022, 9, 1, 0, 0)
            }
        ]

        self.unique_id_amount = set(emp["employee_id"] for emp in self.data_before_group)

    def test_obj_map(self):
        actual = self.obj_mapper(self.data_before_group)
        self.assertEqual(len(self.unique_id_amount), len(actual))

    def test_obj_map_count_record_by_key(self):
        emp_id_1, emp_id_2 = self.unique_id_amount
        actual = self.obj_mapper(self.data_before_group)

        self.assertEqual(3, len(actual[emp_id_1]))
        self.assertEqual(3, len(actual[emp_id_2]))
