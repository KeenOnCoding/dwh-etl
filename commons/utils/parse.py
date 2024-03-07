from urllib import parse


def qs_to_dict(query_string):
    return dict(parse.parse_qsl(query_string))
