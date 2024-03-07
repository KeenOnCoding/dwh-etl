from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class ConnectionDetails:
    url: str
    properties: dict


def get_connection_details(config: Mapping) -> ConnectionDetails:
    host = config.environment.shared.dwh_host
    database = config.environment.shared.dwh_db
    user = config.environment.shared.dwh_user
    password = config.environment.shared.db_pass

    url = f'jdbc:sqlserver://{host};databaseName={database};'

    properties = {
        'user': user,
        'password': password,
        'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
    }
    return ConnectionDetails(url, properties)
