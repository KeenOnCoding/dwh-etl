import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

import pytest
from datetime import datetime

from commons.utils import stringconverters as scnv


@pytest.mark.parametrize('mnemo_to_expected',
                         [
                             ('1d', timedelta(days=1)),
                             ('3d5hr5m31s', timedelta(days=3, hours=5, minutes=5, seconds=31)),
                             ('1 day', timedelta(days=1)),
                             ('1days', timedelta(days=1)),
                             ('1 day 2 minutes', timedelta(days=1, minutes=2)),
                             ('1 day 2minutes5second', timedelta(days=1, minutes=2, seconds=5)),
                             ('1d5hrs 10 seconds', timedelta(days=1, hours=5, seconds=10))
                         ])
def test_mnemonic_str_to_timedelta(mnemo_to_expected):
    mnemo, expected_delta = mnemo_to_expected

    actual_delta = scnv.mnemonic_str_to_timedelta(mnemo)

    assert actual_delta == expected_delta


def now_minus(**kwargs):
    return datetime.now() - relativedelta(**kwargs)


@pytest.mark.parametrize('ago_expr_to_expected',
                         [
                             ('1-day-ago', now_minus(days=1)),
                             ('2-month-ago', now_minus(months=2)),
                             ('10-year-ago', now_minus(years=10)),
                             ('1-week-ago', now_minus(weeks=1))
                         ])
def test_mnemonic_ago(ago_expr_to_expected):
    ago_expr, expected_date = ago_expr_to_expected
    actual_delta = scnv.mnemonic_str_to_timedelta(ago_expr)
    actual_date = datetime.now() - actual_delta

    assert (actual_date - expected_date).seconds <= 1


@pytest.mark.parametrize('wrong_param', [None, 'wrong'])
@pytest.mark.parametrize('conv_func',
                         [
                             scnv.mnemonic_str_to_timedelta,
                             scnv.mnemonic_to_absolute_sec,
                             scnv.dhrms_string_to_timedelta,
                         ])
def test_raises_when_wrong_expr(conv_func, wrong_param):
    with pytest.raises(scnv.StringConversionError):
        conv_func(wrong_param)
