import json
from pathlib import Path
from behave import given, when, then

from commons.configurations.processing_config import HistoricalProcessor
from processor.historical_processor import entrypoint


@given('table "{table_name}"')
def create_and_fill_table(context, table_name):
    context.query_runner.execute('DROP TABLE IF EXISTS ' + table_name)
    table_schema_path = Path(
        __file__).parent / f'../../schema/{table_name}.sql'

    with table_schema_path.open() as schema_file:
        script = schema_file.read()
        context.query_runner.execute(script)

    if context.table is None:
        return

    for row in context.table:
        values = ','.join(val for val in row)
        query = f'INSERT INTO {table_name} VALUES ({values})'
        context.query_runner.execute(query)


@given('I change params "{json_prams}"')
def change_conf(context, json_prams):
    params = json.loads(json_prams)
    context.config.job.processing.history_processor = HistoricalProcessor(**params)


@when('I run the job')
def run_app(context):
    try:
        entrypoint.get_runner(context.config)
    except Exception:
        context.has_err = True
    else:
        context.has_err = False


@then('table "{table_name}" contains')
def table_contains_given_rows(context, table_name):
    query_result = context.query_runner.query(f'SELECT * FROM {table_name}')
    actual_list = list()
    expected_list = list()

    for index, row in enumerate(context.table):
        actual = [str(val) for val in query_result[index].values()]
        actual_list.append(actual)

        expected = [x.strip("'") for x in row.cells]
        expected_list.append(expected)

    assert sorted(actual_list) == sorted(expected_list), \
        f'Actual: {actual_list}, Expected: {expected_list}'


@then('error occur')
def check_for_error(context):
    assert context.has_err
