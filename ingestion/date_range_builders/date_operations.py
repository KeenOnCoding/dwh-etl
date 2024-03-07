from datetime import datetime
from datetime import timedelta
import operator
from dateutil.relativedelta import relativedelta

from ingestion import constants


def add_offset(initial_date, time_offset, unit):
    """Adds offset if stated to initial_date, splits time_offset by creating
    dict {[+,-]amount}
    """

    if time_offset is None:
        return initial_date

    time_offset_dict = {unit: abs(time_offset)}

    delta = relativedelta(
        months=time_offset_dict.get(constants.MONTHS, 0),
        hours=time_offset_dict.get(constants.HOURS, 0),
        days=time_offset_dict.get(constants.DAYS, 0),
        minutes=time_offset_dict.get(constants.MINUTES, 0),
        seconds=time_offset_dict.get(constants.SECONDS, 0)
    )

    return operator.sub(initial_date, delta) if time_offset < 0 else operator.add(initial_date, delta)


def align_date(date: datetime, granularity):
    day = 1
    hour = 0
    minute = 0
    second = 0
    microsecond = 0

    if granularity == constants.MONTHS:
        pass
    elif granularity == constants.DAYS:
        day = date.day
    elif granularity == constants.HOURS:
        day = date.day
        hour = date.hour
    elif granularity == constants.MINUTES:
        day = date.day
        hour = date.hour
        minute = date.minute
    elif granularity == constants.SECONDS:
        day = date.day
        hour = date.hour
        minute = date.minute
        second = date.second
    else:
        raise ValueError(f'No such granularity value: {granularity}')
    date = datetime(date.year, date.month, date.day) if isinstance(date, type(datetime.today().date())) else date
    return date.replace(day=day, hour=hour, minute=minute, second=second, microsecond=microsecond)


def increment_by_unit(datetime_: datetime, unit):
    try:
        return datetime_ + {
            constants.SECONDS: timedelta(seconds=1),
            constants.MINUTES: timedelta(minutes=1),
            constants.HOURS: timedelta(hours=1),
            constants.DAYS: timedelta(days=1),
            constants.MONTHS: relativedelta(months=1)  # months and month have different meanings
            # month will set to January, months - will add one month to the original date
        }[unit]
    except KeyError:
        raise ValueError(f'No such granularity: {unit}')
