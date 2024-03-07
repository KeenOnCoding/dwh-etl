from collections.abc import MutableMapping
from datetime import datetime, timedelta

import pytest

from ingestion.exec.dummy_exec_context import DummyExecutionContext
from ingestion.exec.exec_context import ExecutionContext
from ingestion.exec.incremental_exec_context import IncrementalExecutionContext
from ingestion.exec.snapshot_exec_context import SnapshotExecutionContext


@pytest.mark.parametrize('cls', [ExecutionContext,
                                 DummyExecutionContext,
                                 SnapshotExecutionContext,
                                 IncrementalExecutionContext])
def test_execution_contexts_should_be_mutable_mapping(cls):
    assert issubclass(cls, MutableMapping)


@pytest.mark.parametrize('ctx', [
    DummyExecutionContext(1),
    SnapshotExecutionContext(1),
])
def test_dummy_context_is_hashable(ctx):
    try:
        {ctx}
    except TypeError:
        pytest.fail(f'{ctx} is unhashable type', )


def test_snapshot_execution_context_is_hashable():
    ctx = SnapshotExecutionContext(1)


def test_incremental_context_should_implement_equals_hashcode_contract():
    now = datetime.now()
    year_ago = now.replace(year=now.year - 1)
    delta = timedelta(days=30)

    datetime_list = []
    current_it_datetime = year_ago
    # generate list of datefrom, dateto dictionaries
    while current_it_datetime <= now:
        datetime_list.append((current_it_datetime,current_it_datetime + delta))

        current_it_datetime += delta

    # generate incremental contexts
    incremental_contexts = [IncrementalExecutionContext(1, date_range_dict)
                            for date_range_dict in datetime_list]

    import random
    # pick randint(0, L) items and construct more incremental contexts
    duplicated_datetime_list = random.sample(datetime_list, random.randint(0, len(datetime_list)))
    duplicated_contexts = [IncrementalExecutionContext(1, date_range_dict)
                           for date_range_dict in duplicated_datetime_list]

    # now total contexts contains duplicates
    total_contexts = incremental_contexts + duplicated_contexts

    # there is no duplicates in incremental contexts
    expected_incremental_contexts = len(incremental_contexts)

    # and we are using set to check contract of __eq__ and __hash__
    # assuming we the set() will eliminate all the duplicated contexts
    actual_incremental_contexts = len(set(total_contexts))

    assert actual_incremental_contexts == expected_incremental_contexts
