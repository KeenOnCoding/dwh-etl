import os
import urllib

import sqlalchemy
from commons.db.query_runner import QueryRunner
from commons.configurations.configurator import Configurator

DWH_HOST = 'db'
DWH_DB = 'master'
DWH_USER = 'SA'
DB_PASS = "K~whj<[4-:>3~XSw"


class Holder:
    pass


def before_all(context):
    context.holder = Holder()
    context.config = _get_configuration()
    engine = _create_engine()
    context.query_runner = QueryRunner(None, engine=engine)


def _get_configuration():
    config = Configurator().get_configurator()
    config.environment.shared.dwh_host = DWH_HOST
    config.environment.shared.dwh_db = DWH_DB
    config.environment.shared.dwh_user = DWH_USER
    config.environment.shared.db_pass = DB_PASS
    return config


def _create_engine():
    connect_string = 'DRIVER={ODBC Driver 17 for SQL Server};' \
                     f'SERVER={DWH_HOST};' \
                     f'PORT=1433;' \
                     f'DATABASE={DWH_DB};' \
                     f'UID={DWH_USER};' \
                     f'PWD={DB_PASS};'

    params = urllib.parse.quote_plus(connect_string)
    return sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect=%s" % params,
        convert_unicode=True)
