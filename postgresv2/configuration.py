from .dynamic_params import get_tables


CONFIG = {
    'title': 'Postgres',
    "params": [
        {
            "name": "host",
            "title": "Host",
            "type": "string",
            "placeholder": "IP (e.g. 123.45.67.89) or "
                           "hostname (e.g. your.server.com)",
            "required": True,
            'help': 'The IP address or hostname of your Postgres database.'
                    'May require whitelisting.  \n\n'
                    '[Learn more](https://panoply.io/docs/data-sources/postgresql/).',  # noqa
            'description': "May require whitelisting Panoply.[Learn more]"
                           "(https://panoply.io/docs/data-security/whitelisting/)."  # noqa
        },
        {
            "name": "port",
            "title": "Port",
            "type": "number",
            "placeholder": "e.g. 5432",
            'default': "5432",
            "help": "The port number of your Postgres server. "
                    "This is `5432` for most users.",
            "required": True
        },
        {
            "name": "username",
            "title": "Username",
            "placeholder": "Username",
            "type": "string",
            "help": "Enter your Postgres username.",
            "required": True
        },
        {
            "name": "password",
            "title": "Password",
            "placeholder": "Password",
            "type": "password",
            "help": "Enter your Postgres password.",
            "required": True
        },
        {
            "name": "db_name",
            "title": "Database Name",
            'type': 'string',
            "placeholder": "Database Name",
            "required": True,
            "help": "Select the Postgres database from where to collect data.",
            "description": "Learn how to [find the database name]"
                           "(https://dba.stackexchange.com/questions/58312/how-to-get-the-name-of-the-current-database-from-within-postgresql)."  # noqa
        },
        {
            "name": "data_available",
            "required": True,
            "title": "Data Available",
            "type": "dynamic-list",
            "values": get_tables,
            "help": "Select the Postgres data tables or views to collect.",
            "dependencies": ["host", "port", "username", "password", "db_name"]
        },
        {
            'name': 'schema',
            'title': 'Schema',
            'type': 'schema',
            'required': True,
            'category': 'advanced'
        },
        {
            'name': 'destination',
            'title': 'Destination',
            'type': 'destination',
            'category': 'advanced'
        },
        {
            'name': 'idpattern',
            'title': 'Primary Key',
            'help': 'Primary Keys are the column(s) values that uniquely '
                    'identify a row. Once identified Panoply upserts new '
                    'data and prevents duplicate data.  \n\n'
                    'Panoply automatically selects the Primary Key using the '
                    'available ID columns. If none are available, you may '
                    'configure this manually by choosing the columns '
                    'to use.  \n\n'
                    '[Learn more](https://panoply.io/docs/manage-data/primary-keys/).',  # noqa
            'description': 'Enter {column} names',
            'type': 'primary-key',
            'category': 'advanced'
        },
        {
            'name': 'inckey',
            'title': 'Incremental Key',
            'help': 'The Incremental Key identifies the point from which to '
                    'collect data incrementally from the source. Said '
                    'differently, it identifies the point from which new '
                    'records are inserted in the destination table. '
                    'Configuring this is useful to speed up the collection. '
                    'By default, Panoply does not use an Incremental Key. '
                    'Instead Panoply extracts all of your Postgres data on '
                    'each collect.  \n\n'
                    'To collect incrementally enter a column, and optionally'
                    ' a value within that column, which identify the '
                    'incremental collection point. Pay special attention to '
                    'formatting as it must match what is found in the data '
                    'source.  \n\n'
                    '[Learn more](https://panoply.io/docs/manage-data/incremental-key/).',  # noqa
            'type': 'incremental-key',
            'description': 'Enter a column name',
            'category': 'advanced'
        },
        {
            'name': 'excludes',
            'title': 'Exclude',
            'help': 'When collecting data, you may want to exclude certain '
                    'data, such as names, addresses, or other personally '
                    'identifiable information. Enter the column names of '
                    'the data to exclude.',
            'type': 'exclude',
            'category': 'advanced'
        },
        {
            'name': 'stringFields',
            'title': 'Parse String',
            'help': 'If the data to be collected contains JSON, include the '
                    'JSON text attributes to be parsed.',
            'type': 'parse-string',
            'category': 'advanced'
        },
        {
            'name': 'truncateTable',
            'title': 'Truncate Table',
            'help': 'Truncate deletes all the current data stored in the '
                    'destination tables, but not the tables themselves. '
                    'Afterwards Panoply will recollect all the available data '
                    'for this data source.',
            'description': 'Enable Truncate Table',
            'type': 'truncate-table',
            'category': 'advanced'
        }
    ],
    'categories': ['DB', 'POPULAR'],
    'keywords': ['db', 'database', 'sql'],
    'createdAt': '2020-08-02',
    'hostProperty': 'host'
}
