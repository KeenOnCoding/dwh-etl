import urllib
import sqlalchemy


def create_engine(config):
    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};" \
                        f"SERVER={config.environment.shared.dwh_host};"\
                        f"PORT=1443;" \
                        f"DATABASE={config.environment.shared.dwh_db};" \
                        f"UID={config.environment.shared.dwh_user};" \
                        f"PWD={config.environment.shared.db_pass};" \
                        f"authenticationSchem=nativeAuthentication;"
    quoted_connection_string = urllib.parse.quote_plus(connection_string)

    return sqlalchemy.create_engine(
        f'mssql+pyodbc:///?odbc_connect={quoted_connection_string}',
        convert_unicode=True
    )
