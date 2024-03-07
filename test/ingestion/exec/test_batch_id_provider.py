import pytest

from ingestion.exec.batch import BatchIdProvider


@pytest.mark.parametrize('repeats', [2, 1_000])
def test_should_always_return_uniques(repeats):
    ids = []
    for i in range(repeats):
        batch_id = BatchIdProvider.get_batch_id()
        ids.append(batch_id)

    assert len(ids) == len(set(ids))
