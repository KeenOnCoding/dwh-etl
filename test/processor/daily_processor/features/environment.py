import urllib
import sqlalchemy

from commons.configurations.configurator import Configurator

DWH_HOST = 'db'
DWH_DB = 'master'
DWH_USER = 'SA'
DB_PASS = "K~whj<[4-:>3~XSw"


def before_all(context):
    context.config = _get_configuration()
    context.engine = _create_engine()

def _get_configuration():
    config = Configurator().get_configurator()
    config.environment.shared.dwh_host = DWH_HOST
    config.environment.shared.dwh_db = DWH_DB
    config.environment.shared.dwh_user = DWH_USER
    config.environment.shared.db_pass = DB_PASS
    config.job.processing.daily_processor.daily_proc_target_table = 'dbo.daily_split'
    config.job.processing.daily_processor.daily_proc_source_table = 'dbo.no_split'
    config.job.processing.daily_processor.date_column = 'started'
    config.job.processing.daily_processor.batch_window = 3
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
