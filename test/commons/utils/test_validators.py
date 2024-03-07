import random

import pytest
from commons.utils import validators


def test_should_raise_when_none():
    with pytest.raises(validators.RequireNotNoneException):
        validators.require_not_none(None, 'the value required to be not None')


def test_should_not_raise_when_not_none():
    expected_value = random.randint(0, 1_000)
    actual_value = validators.require_not_none(expected_value)
    assert actual_value == expected_value
