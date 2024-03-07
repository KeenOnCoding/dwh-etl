import traceback
from pathlib import Path

from behave import *
from sqlalchemy.orm import sessionmaker
from processor.daily_processor.start import get_runner
from commons.logging.simple_logger import logger


@given('table "{table_name}"')
def create_table(context, table_name):
    execute_database_query(context.engine,
                           "DROP TABLE IF EXISTS " + table_name)
    table_schema_path = Path(
        __file__).parent / f"../../schema/table_{table_name}.sql"

    with table_schema_path.open() as schema_file:
        script = schema_file.read()
        execute_database_query(context.engine, script)


@given('table "{table_name}" contains following records')
def fill_table_with_records(context, table_name):
    for row in context.table:
        values = list(map(lambda val: f"{val}", row))
        values = ",".join(values)
        query = f"INSERT INTO {table_name} VALUES ({values})"
        execute_database_query(context.engine, query)


@when('I run the job')
def run_app(context):
    try:
        get_runner(context.config).run(
            source_table=context.config.job.processing.daily_processor.daily_proc_source_table,
            expected_calendar_time_table="dbo.working_hours"
        )
    except Exception as e:
        traceback.print_exc()
        context.exc = e
        context.err = True


@then('table "{table_name}" contains "{row_number}" row')
def table_should_contain_given_number_of_rows(context, table_name, row_number):
    query_result = query_database(context.engine,
                                  f"SELECT COUNT(*) FROM {table_name}")
    actual_len = int(query_result[0][0])

    assert int(query_result[0][0]) == int(row_number), \
        f"Actual: {actual_len}, Expected: {int(row_number)}"


@then('table "{table_name}" contains')
def table_should_contains_give_rows(context, table_name):
    query_result = query_database(context.engine,
                                  f"SELECT * FROM {table_name}")

    for index, row in enumerate(context.table):
        db_record = query_result[index].values()
        db_record = [str(val) for val in db_record]
        table_record = row.cells
        table_record = [x.strip("'") for x in table_record]

        # fifth record is a boolean that is extracted from the scenario.
        # It should be converted from string to int and then to a string
        # representation of boolean to be used when comparing rows.
        table_record[5] = f"{bool(int(table_record[5]))}"
        table_record[7] = f"{bool(int(table_record[7]))}"

        assert db_record == table_record, \
            f"Actual: {db_record} Expected: {table_record}"


def query_database(engine, query):
    """Returns a list of queried rows"""
    with engine.connect().execution_options(
            isolation_level="AUTOCOMMIT") as connection:
        result = connection.execute(query)
        return list(result)


def execute_database_query(engine, query):
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        session.execute(query)

        session.commit()
    except Exception as e:
        session.rollback()
        logger.error("An error occurred when trying"
                     f" to executed the following query: {query};\n"
                     f"Error: {e}\n"
                     f"Rolling back and raising.")
        raise
    finally:
        session.close()
