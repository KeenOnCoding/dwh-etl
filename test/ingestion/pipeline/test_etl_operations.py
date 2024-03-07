import pytest

from ingestion.constants import OUT_TABLE
from ingestion.pipeline.operation import etl as op


@pytest.mark.usefixtures('countable')
@pytest.mark.usefixtures('count_data')
def test_count_operation(countable, count_data):
    operation = op.CountOperation()
    is_err, actual_result = operation.invoke(payload=countable)

    assert not is_err
    assert actual_result == count_data


@pytest.mark.usefixtures('countable', 'count_data')
def test_count_to_context_operation(countable, count_data):
    ctx = {}
    ctx_key = 'key'
    operation = op.CountToContext(ctx_key)
    is_err, res = operation.invoke(payload=countable, context=ctx)

    assert not is_err
    assert countable == res
    assert ctx[ctx_key] == count_data


@pytest.mark.usefixtures('data_source', 'countable')
def test_extract_data_operation(data_source, countable):
    operation = op.ExtractDataOperation(data_source)
    expected_extracted = countable
    is_err, actual_extracted = operation.invoke(context={})

    assert not is_err
    assert actual_extracted == expected_extracted


@pytest.mark.usefixtures('enricher', 'countable')
def test_enricher_operation(enricher, countable):
    expected_after_enrichment = countable
    operation = op.EnrichDataOperation(enricher)
    is_err, actual_after_enrichment = operation.invoke()

    assert not is_err
    assert actual_after_enrichment == expected_after_enrichment


@pytest.mark.usefixtures('sink', 'data')
def test_load_data_operation(sink, data):
    table = 'some-table'
    expected_data_to_be_saved = data
    ctx = {
        OUT_TABLE: table
    }

    operation = op.LoadDataOperation(sink)
    is_err, res = operation.invoke(payload=expected_data_to_be_saved, context=ctx)

    assert not is_err
    assert sink.state == expected_data_to_be_saved
    assert sink.table_name == table
