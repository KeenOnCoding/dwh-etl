import re
from datetime import timedelta, datetime
from functools import lru_cache


class StringConversionError(Exception):
    pass


@lru_cache(maxsize=12)
def dhrms_string_to_timedelta(dhrms_str) -> timedelta:
    if dhrms_str is None:
        raise StringConversionError('Cannot parse None values to timedelta')

    pattern = re.compile(
        r'((?P<days>\d+?) ?d(ay)?[s]?)?'
        r' ?((?P<hours>\d+?) ?(hr|hour)s?)?'
        r' ?((?P<minutes>\d+?) ?m(inute)?s?)?'
        r' ?((?P<seconds>\d+?) ?s(econd[s]?)?)?'
    )
    parts = pattern.match(dhrms_str).groupdict()

    if not any(value is not None for value in parts.values()):
        raise StringConversionError(f"Cannot parse the string: [{dhrms_str}]. "
                                    f"Examples of threshold format: '5d2hr3m23s'/'1 day 2 second'/'3minutes2second'")
    time_params = dict()

    for name, param in parts.items():
        if param:
            time_params[name] = int(param)

    return timedelta(**time_params)


def dhrms_string_to_total_seconds(dhrms_str) -> float:
    return dhrms_string_to_timedelta(dhrms_str).total_seconds()


def _n_what_ago(mnemonic_str):
    amount, time_part, _ = mnemonic_str.split('-')
    now = datetime.now()
    amount = int(amount)

    part_mapping = {
        "year": now - now.replace(year=(now.year - amount)),
        "week": timedelta(days=7 * amount),
        "day": timedelta(days=1 * amount),
        "month": timedelta(days=31 * amount)
    }
    return part_mapping[time_part]


def _current_what(mnemonic_str):
    _, time_part = mnemonic_str.split('-')
    now = datetime.now()

    part_mapping = {
        'month': now - now.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
        'year': now - now.replace(day=1, month=1),
        'day': now - now.replace(hour=0, minute=0, second=0, microsecond=0),
        'week': now - now.replace(day=(now.day - now.weekday())),
    }
    return part_mapping[time_part]


@lru_cache(maxsize=12)
def mnemonic_str_to_timedelta(mnemonic_str) -> timedelta:
    if mnemonic_str is None:
        raise StringConversionError('Cannot parse None value to timedelta')
    mnemonic_markers = {
        'current': _current_what,
        'ago': _n_what_ago
    }

    for marker, convert_function in mnemonic_markers.items():
        if marker in mnemonic_str:
            return convert_function(mnemonic_str)

    return dhrms_string_to_timedelta(mnemonic_str)


def mnemonic_to_absolute_sec(mnemonic_str) -> int:
    return abs(int(mnemonic_str_to_timedelta(mnemonic_str).total_seconds()))
