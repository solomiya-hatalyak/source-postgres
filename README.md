# Panoply.io Postgres Data Source

This is Panoply.io's Postgres data source connector.

See [Panoply's Python SDK documentation](https://github.com/panoplyio/panoply-python-sdk) to understand how these data source objects work and how to build your own.

The implementation uses [Postgres cursors](https://www.postgresql.org/docs/9.2/static/plpgsql-cursors.html) so its relatively performant even on tables without indexes.

## Input
The input to the data source is a dictionary that describes the source details such as host, port, database name, list of tables to pull and incremental key & value (if applicable).

An example input might look like this:
```python
my_source = {
    "addr": "postgres.domain.com[:port]/database_name",
    "user": "[YOUR USERNAME]",
    "password": "[YOUR PASSWORD]",
    "tables": [
        {"value": "public.t1"},
        {"value": "public.t2"},
        {"value": "some_schema.t3"}
    ]
}
```

The list of tables is a list of dictionaries which must contain a `value` key.
The name of the table must include the schema name separate by a dot (`.`).

## Output
### Reading from the stream
You read from the stream by making consecutive calls to the `read` method until it returns `None`.

This data source will output a list of rows from the input tables. A single batch is returned with every call to `read` (batch size is determined by the `n` argument to `read` - defaults to 5000 records).
Each item in the list is a dictionary representing that row.

To each row we also append the schema name and table where that row originated from (since the stream reads all tables consecutively) under the keys `__schemaname` and `__tablename` respectively.


### Listing tables
The stream can also be used to get a list of tables and views from the source by calling the `get_tables` method:

```python
stream = Postgres(my_source, OPTIONS)
tables = stream.get_tables()
# tables = [
#  { name: 'public.t1', value: 'public.t1'}
#  { name: 'myschema.v1 (VIEW)', value: 'myschema.v1'}
# ]
```
It also returns views because views can be queries and ingested just like regular tables as far as the stream is concerned.
The `name` key specifies which is a view and which is table, however the `value` parameter is returned in the plain format (ready to be used as input to the stream).


## Contributing
We'll gladly accept any contributions as long as:
1. Your changes are generic and not too specific to you (remember this serves many customers)
1. You've added tests
1. You've documented them
1. It is backward compatible (if its not backward compatible use the `source.version` parameter to determine behaviour and contact us to coordinate)
