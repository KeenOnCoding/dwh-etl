from typing import Any
from typing import Hashable, Mapping


class RequireNotNoneException(RuntimeError):
    pass


class RequireSchemaPresenceException(RuntimeError):
    pass


class NoSuchKeyFoundException(RuntimeError):
    pass


def require_not_none(value, message='') -> Any:
    if value:
        return value

    raise RequireNotNoneException(message or 'Value should not be None')


def extract_not_none(dictlike: Mapping, key: Hashable) -> Any:
    sentinel = object()
    value = dictlike.get(key, sentinel)
    if value is None:
        raise RequireNotNoneException(f'Got None on key: {key}')

    if value is sentinel:
        raise NoSuchKeyFoundException(f'No such key in configuration: {key}')

    return value


def require_schema_presence(table: str) -> str:
    try:
        schema, _ = table.split('.')
        return table
    except ValueError:
        raise RequireSchemaPresenceException(f'No schema was declared: {table}')
