import yaml
from yaml.scanner import ScannerError


class RuleReader:
    __slots__ = '__schema_to_rules'

    def __init__(self, raw_rules):
        try:
            rules = yaml.safe_load(raw_rules)
            if rules is None:
                raise ScannerError("No rules found")
            self.__schema_to_rules = self.__unpack(rules)
        except ScannerError as e:
            raise ValueError(f'Error occurred on loading rules: {e}')
        except KeyError as e:
            raise RuntimeError(f'Root object "rules" not found: {e}')
        except RuntimeError as e:
            raise e

    def get_schemas(self) -> list:
        return list(self.__schema_to_rules)

    def get_schema_rule_plan(self, schema_name: str) -> dict:
        try:
            return self.__schema_to_rules[schema_name]
        except KeyError:
            raise ValueError(f'Rules to {schema_name} not found')

    def __unpack(self, rules) -> dict:
        rules = rules['rules']
        schema_to_rules = dict()

        # for each schema found in rules
        for raw_schema_to_rules in rules:
            schema = list(raw_schema_to_rules)
            if len(schema) != 1:
                raise RuntimeError("Root object 'rules' should contain "
                                   "list of children with only "
                                   "1 key-value pair (schema-rules). "
                                   f"Found schemas in child item: {schema}")
            schema = schema[0]
            # map schema to rules
            schema_to_rules[schema] = raw_schema_to_rules[schema]

        return schema_to_rules
