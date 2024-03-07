import pytest

from ingestion.pipeline.builder import EtlPipelineBuilder
from ingestion.constants import IN_AMOUNT, OUT_AMOUNT, OUT_TABLE


@pytest.mark.usefixtures('enricher', 'data_source', 'sink', 'count_data', 'countable')
def test_etl_pipeline_flow(enricher, data_source, sink, countable, count_data):
    pipeline = EtlPipelineBuilder(sink=sink, datasource=data_source, enricher_chain=enricher).build()

    expected_out_table_name = 'out'

    ctx = {
        OUT_TABLE: expected_out_table_name
    }
    is_err, result = pipeline.invoke(context=ctx)
    assert not is_err

    actual_out_table_name = sink.table_name
    assert actual_out_table_name == expected_out_table_name

    expected_sunk_data = countable
    actual_sunk_data = sink.state
    assert actual_sunk_data == expected_sunk_data

    expected_in_amount, expected_out_amount = count_data, count_data
    actual_in_amount, actual_out_amount = ctx[IN_AMOUNT], ctx[OUT_AMOUNT]

    assert actual_in_amount == expected_in_amount
    assert actual_out_amount == expected_out_amount
