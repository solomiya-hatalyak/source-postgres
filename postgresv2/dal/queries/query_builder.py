from typing import Any


def get_query(schema: str, table: str, inckey: str, incval: Any,
              max_value: Any) -> str:
    """return a SELECT query using properties from the source"""
    where = ''
    orderby = get_orderby(inckey)

    if inckey and incval:
        inc_clause = get_incremental(inckey, incval, max_value)
        where = "{}{}".format(where, inc_clause)

    if where:
        where = ' WHERE {}'.format(where)

    return 'SELECT * FROM "{}"."{}"{}{}'.format(
        schema, table, where, orderby
    )


def get_orderby(inckey: str) -> str:
    orderby = ''
    if inckey:
        orderby = ' ORDER BY "{}"'.format(inckey)
    return orderby


def get_incremental(inckey: str, incval: Any, max_value: Any) -> str:
    inc_clause = "\"{}\" >= '{}'".format(inckey, incval)
    if max_value:
        inc_clause = "({} AND \"{}\" <= '{}')".format(
            inc_clause,
            inckey,
            max_value
        )

    return inc_clause


def get_max_value_query(column: str, schema: str, table: str) -> str:
    return 'SELECT MAX("{}") FROM "{}"."{}"'.format(
            column,
            schema,
            table
        )
