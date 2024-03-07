from datetime import datetime

import pytest

from ingestion import constants
from ingestion.date_range_builders.date_operations import add_offset, align_date, increment_by_unit


def test_increment_by_month_granularity():
    date = datetime(2021, 12, 1)
    expected_date = date.replace(year=2022, month=1)
    actual_date = increment_by_unit(date, constants.MONTHS)

    assert actual_date == expected_date


def test_increment_by_minutes():
    date = datetime(2022, 4, 27, 15, 1)
    expected_date = date.replace(minute=2)
    actual_date = increment_by_unit(date, constants.MINUTES)

    assert actual_date == expected_date


def test_increment_by_second():
    date = datetime(2022, 4, 27, 15, 1, 1)
    expected_date = date.replace(second=2)
    actual_date = increment_by_unit(date, constants.SECONDS)

    assert actual_date == expected_date


def test_increment_by_day():
    date = datetime(2021, 12, 31)
    expected_date = date.replace(year=2022, month=1, day=1)
    actual_date = increment_by_unit(date, constants.DAYS)

    assert actual_date == expected_date


def test_increment_by_hour():
    date = datetime(2021, 12, 31, 23)
    expected_date = date.replace(year=2022, month=1, day=1, hour=0)
    actual_date = increment_by_unit(date, constants.HOURS)

    assert actual_date == expected_date


def test_wrong_granularity_passed():
    wrong_granularity = 'wrong'
    with pytest.raises(ValueError):
        increment_by_unit(datetime.now(), wrong_granularity)


def create_datetime(granularity):
    base_datetime = datetime(year=2022, month=2, day=14, hour=11)
    return {
        constants.MONTHS: (base_datetime, constants.MONTHS, datetime(2022, 2, 1, 0)),
        constants.DAYS: (base_datetime, constants.DAYS, datetime(2022, 2, 14, 0)),
        constants.HOURS: (base_datetime, constants.HOURS, datetime(2022, 2, 14, 11))

    }[granularity]


@pytest.mark.parametrize('granularity_datetime_to_expected_datetime',
                         [
                             create_datetime(constants.HOURS),
                             create_datetime(constants.MONTHS),
                             create_datetime(constants.DAYS)
                         ])
def test_align_dates_with_granularity(granularity_datetime_to_expected_datetime):
    date, granularity, expected_result = granularity_datetime_to_expected_datetime
    actual_result = align_date(date, granularity)
    assert actual_result == expected_result


def test_align_dates_raises_when_wrong_granularity():
    wrong_granularity = 'wrong'
    with pytest.raises(ValueError):
        align_date(datetime.now(), wrong_granularity)


def test_add_offset():
    expected = datetime(2020, 10, 18, 10, 00, 00)
    actual = add_offset(datetime(2020, 10, 18, 00, 00, 00), 10, constants.HOURS)

    assert actual == expected
