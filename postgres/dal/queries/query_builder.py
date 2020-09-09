
def get_query(schema, table, inckey, incval, keys, max_value, state=None) -> str:
    """return a SELECT query using properties from the source"""
    where = ''
    orderby = get_orderby(keys, inckey)

    if state:
        where = use_indexes(state)

    if inckey and incval:
        if where:
            where = '{} AND '.format(where)

        inc_clause = get_incremental(where, inckey, incval, max_value)
        where = "{}{}".format(where, inc_clause)

    if where:
        where = ' WHERE {}'.format(where)

    return 'SELECT * FROM "{}"."{}"{}{}'.format(
        schema, table, where, orderby
    )


def get_orderby(keys, inckey):
    orderby = ''
    if keys:
        keys = [key.get('attname') for key in keys]
        if inckey and inckey not in keys:
            keys.append(inckey)
        keys = map(lambda i: '"{}"'.format(i), keys)
        orderby = " ORDER BY {}".format(','.join(keys))
    return orderby


def use_indexes(state) -> str:
    multi_column_index = len(state)
    where = "{} >= {}"

    if multi_column_index > 1:
        where = '({}) >= ({})'
    keys = map(lambda i: '"{}"'.format(i), state.keys())
    where = where.format(
        ','.join(keys),
        ','.join(map(lambda x: "'{}'".format(x), state.values()))
    )

    return where


def get_incremental(where, inckey, incval, max_value) -> str:
    if inckey not in where:
        inc_clause = "\"{}\" >= '{}'".format(inckey, incval)
        if max_value:
            inc_clause = "({} AND \"{}\" <= '{}')".format(
                inc_clause,
                inckey,
                max_value
            )
    else:
        inc_clause = "{} <= '{}'".format(inckey, max_value)

    return inc_clause


def get_max_value_query(column: str, schema: str, table: str) -> str:
    return 'SELECT MAX("{}") FROM "{}"."{}"'.format(
            column,
            schema,
            table
        )
