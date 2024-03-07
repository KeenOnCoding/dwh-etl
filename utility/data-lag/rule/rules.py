import re
import sys
from typing import List, Dict, Hashable, Tuple
from enum import Enum

from lag.lag_payload import PayloadColumn


def bind_external_parameters(*args: Tuple[str, bool]):
    """
    The decorator is used to bind external parameter names with function
    arguments.

    :param args: list of paired tuples (str, boolean)
        that contains external parameter name
        and a boolean value representing necessity of the argument.
        False - the logic can cope with absence of an argument
        True - the argument is required
    """

    def register_external_parameters(func):
        func._bound_params = dict()
        for parameter, required in args:
            func._bound_params[parameter] = required
        return func

    return register_external_parameters


def order(exec_order):
    """
    Sets an order integer parameter to the decorated function.

    :param exec_order: integer value represents that is set to the function
    object
    """

    def register_order(func):
        func._order = exec_order
        return func

    return register_order


def __update_list_of_dicts(key: Hashable,
                           value_generator: callable,
                           list_of_dictionaries: List[Dict],
                           table_name_predicate: callable,
                           ) -> List[Dict]:
    """
    Update each dict in the list
    Each dict is required to have key-value pair with PayloadColumn.TABLE_NAME
    key and the table name value

    :param key: new dict key
    :param value_generator: function that accepts table name and returns
        appropriate value that will be set with supplied key
    :param list_of_dictionaries: list of dictionaries to be updated
    :param table_name_predicate: function that accepts table name and returns
        boolean value whether to update the dict with key-value pair, where
        key is this functions argument and value is generated with
        value_generator

    :return: list of dictionaries
    """
    result_list = list()
    for item in list_of_dictionaries:
        current_table_name = item[PayloadColumn.TABLE_NAME]
        if table_name_predicate(current_table_name):
            value = value_generator(current_table_name)
            item = dict(item, **{key: value})

        result_list.append(item)

    return result_list


@order(1)
@bind_external_parameters(("default-partition-column", True))
def rule_add_default_partition_column(partition_column_value: str,
                                      tables: List[Dict]) -> List[Dict]:
    """
    Sets the partition_column_value in each dict in list, the key
    is PayloadColumn.PARTITION_COLUMN.

    :param partition_column_value: value to set
    :param tables: list of table dictionary wrappers
    :return: modified list of table dictionary wrappers
    """
    return __update_list_of_dicts(
        PayloadColumn.PARTITION_COLUMN,
        lambda *_: partition_column_value,
        tables,
        lambda *_: True)


@order(2)
@bind_external_parameters(("default-threshold-value", True))
def rule_add_default_threshold_value(threshold_value,
                                     tables: List[Dict]) -> List[Dict]:
    """
     Sets the partition_column_value in each dict in list, the key
     is PayloadColumn.THRESHOLD_VALUE.

     :param threshold_value: value to set
     :param tables: list of table dictionary wrappers
     :return: modified list of table dictionary wrappers
     """
    return __update_list_of_dicts(
        PayloadColumn.THRESHOLD_VALUE,
        lambda *_: threshold_value,
        tables,
        lambda *_: True)


@order(3)
@bind_external_parameters(("default-check-strategy", True))
def rule_add_default_check_strategy(check_strategy,
                                    tables: List[Dict]) -> List[Dict]:
    return __update_list_of_dicts(
        PayloadColumn.CHECK_STRATEGY,
        lambda *_: check_strategy,
        tables,
        lambda *_: True)


def __do_general_regex_filter(regex: str,
                              keep: bool,
                              tables: List[Dict]) -> List[Dict]:
    """
    Filters tables by the regex.

    :param regex: regex that will be applied for each PayloadColumn.TABLE_NAME
    :param keep: if True - will keep only those entries which match the regex,
        if False - will discard entries that match the regex expression
    :param tables: list of dictionaries

    :return: filtered list of dictionaries
    """
    pattern = re.compile(regex)

    def get_filter(is_positive):
        if is_positive:
            return lambda x: bool(pattern.search(x))
        else:
            return lambda x: not bool(pattern.search(x))

    filter_function = get_filter(keep)

    return [item for item in tables if filter_function(
        item[PayloadColumn.TABLE_NAME])]


def __join_sequence_on(join_string: str, seq: Tuple[str, ...]) -> str:
    # sequence can be a string if only one value is specified
    # so we returning back this string
    if isinstance(seq, str):
        return seq
    return join_string.join(seq)


def __prepare_prefix_regex(prefixes: Tuple[str, ...]) -> str:
    """
    Prepares prefix OR regex from the given strings.

    For example, let the prefixes be ('I', 'AM', 'GROOT').
    The function will return fr'^(I|AM|GROOT)' regex string


    :param prefixes: tuple of strings
    :return: prefix OR regex
    """
    return fr"^({__join_sequence_on('|', prefixes)})"


def __prepare_postfix_regex(postfixes: Tuple[str, ...]) -> str:
    """
       Prepares postfix OR regex from the given strings.

       For example, let the prefixes be ('I', 'AM', 'GROOT').
       The function will return fr'(I|AM|GROOT)$' regex string


       :param postfixes: tuple of strings
       :return: prefix OR regex
       """
    return fr"({__join_sequence_on('|', postfixes)})$"


@order(4)
@bind_external_parameters(("allowed-prefixes", False))
def rule_keep_this_prefixes(prefixes: Tuple[str, ...],
                            tables: List[Dict]) -> List[Dict]:
    prefix_regex = __prepare_prefix_regex(prefixes)

    return __do_general_regex_filter(regex=prefix_regex,
                                     keep=True,
                                     tables=tables)


@order(5)
@bind_external_parameters(("forbidden-prefixes", False))
def rule_discard_this_prefixes(prefixes: Tuple[str, ...],
                               tables: List[Dict]) -> List[Dict]:
    prefix_regex = __prepare_prefix_regex(prefixes)

    return __do_general_regex_filter(regex=prefix_regex,
                                     keep=False,
                                     tables=tables)


@order(6)
@bind_external_parameters(("allowed-postfixes", False))
def rule_keep_this_postfixes(postfixes: Tuple[str, ...],
                             tables: List[Dict]) -> List[Dict]:
    postfix_regex = __prepare_postfix_regex(postfixes)

    return __do_general_regex_filter(regex=postfix_regex,
                                     keep=True,
                                     tables=tables)


@order(7)
@bind_external_parameters(("forbidden-postfixes", False))
def rule_discard_this_postfixes(postfixes: Tuple[str, ...],
                                tables: List[Dict]) -> List[Dict]:
    postfix_regex = __prepare_postfix_regex(postfixes)

    return __do_general_regex_filter(regex=postfix_regex,
                                     keep=False,
                                     tables=tables)


@order(7)
@bind_external_parameters(("ignore-tables", False))
def rule_remove_ignored_tables(ignored_tables: Tuple[str, ...],
                               tables: List[Dict]) -> List[Dict]:
    ignored_tables = fr"{__join_sequence_on('|', ignored_tables)}"

    return __do_general_regex_filter(regex=ignored_tables,
                                     keep=False,
                                     tables=tables)


def __merge_to_single_dict(data: List[Dict]) -> Dict:
    """
    Merges list of dictionaries to a single dictionary

    :param data: list of dicts with unique keys
    :return: merged dictionary
    """
    result = dict()
    for dictionary in data:
        result.update(dictionary)
    return result


def __create_value_generator(table_names_substrings_to_values: dict):
    """
    Returns value generator that generates value according to
    supplied table name.

    For example, we can have rule config that overrides default partition
    column of tables that contains in name word OVERRIDES

    Input tables [dim_OVERRIDES, OVERRIDES, some_table]
    Input config [OVERRIDES: this_partition_column]

    So, the generator returns 'this_partition_column' only for the tables
    that contains OVERRIDES inside.
    """
    table_substrings = list(table_names_substrings_to_values)

    def get_override_value_for_table_name(table_name: str):
        # get the first value from the substrings that is a substring
        # of a table_name
        override_value_key = next(
            filter(lambda substring: substring in table_name,
                   table_substrings))

        return table_names_substrings_to_values[override_value_key]

    return get_override_value_for_table_name


def __create_table_name_predicate(table_name_to_column_value):
    return lambda table_name: any(
        table_substring in table_name for table_substring in
        list(table_name_to_column_value))


def __generate_override_function(payload: List[Dict],
                                 override_param_name: Enum) -> callable:
    table_name_to_column_value = __merge_to_single_dict(payload)
    value_generator = __create_value_generator(table_name_to_column_value)
    table_name_acceptance_predicate = __create_table_name_predicate(
        table_name_to_column_value)

    return lambda tables: __update_list_of_dicts(
        key=override_param_name,
        value_generator=value_generator,
        list_of_dictionaries=tables,
        table_name_predicate=table_name_acceptance_predicate
    )


@order(8)
@bind_external_parameters(("partition-column-overrides", False))
def rule_override_partition_column(payload: List[Dict],
                                   tables: List[Dict]) -> List[Dict]:
    return __generate_override_function(
        payload,
        override_param_name=PayloadColumn.PARTITION_COLUMN)(tables)


@order(9)
@bind_external_parameters(("threshold-values-overrides", False))
def rule_override_threshold_value(payload: List[Dict],
                                  tables: List[Dict]) -> List[Dict]:
    return __generate_override_function(
        payload,
        override_param_name=PayloadColumn.THRESHOLD_VALUE)(tables)


@order(10)
@bind_external_parameters(("check-strategy-overrides", False))
def rule_override_check_strategy(payload: List[Dict],
                                 tables: List[Dict]) -> List[Dict]:
    return __generate_override_function(
        payload,
        override_param_name=PayloadColumn.CHECK_STRATEGY)(tables)


this_module = sys.modules[__name__]


def get_rule_order():
    """
    Locates functions prefixed with 'rule' in rules.py returns sorted list
    of its names
    :return: sorted list of rule_names
    """
    rule_names_with_order = [(rule_name,
                              getattr(this_module, rule_name)._order)
                             for rule_name in dir(this_module)
                             if rule_name.startswith("rule")]
    sorted_by_order = sorted(rule_names_with_order, key=lambda x: x[1])
    return [x[0] for x in sorted_by_order]


def get_registered_rules():
    """
    :return: rule name mapping to its function
    """
    return {rule: getattr(this_module, rule) for rule in get_rule_order()}
