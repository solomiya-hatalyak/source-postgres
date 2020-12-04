

def get_query(schema: str, table: str, keys: list,
              last_values: (list, None)) -> str:
    """return a SELECT query using properties from the source"""
    where = get_where(keys, last_values)
    orderby = get_orderby(keys)

    return 'SELECT * FROM "{}"."{}"{}{}'.format(
        schema, table, where, orderby
    )


def get_orderby(keys: list) -> str:
    orderby = ''
    if keys:
        columns = ['"{}"'.format(column) for column in keys]
        columns = ', '.join(columns)
        orderby = ' ORDER BY {}'.format(columns)
    return orderby


def get_where(keys: list, last_values: (list, None)) -> str:
    where = ''
    if last_values:
        conditions = ["\"{}\" >= '{}'".format(column, value)
                      for column, value in zip(keys, last_values)]
        conditions = ' AND '.join(conditions)
        where = ' WHERE {}'.format(conditions)
    return where
