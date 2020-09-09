from .dynamic_params import  get_tables


CONFIG = {
    'title': 'Postgres',
    "params": [
        {
            "name": "host",
            "title": "Host",
            "type": "string",
            "placeholder": "IP (e.g. 123.45.67.89) or  hostname (e.g. your.server.com)",
            "required": True,
            'help': 'May require whitelisting Panoply. Learn more.'
        },
        {
            "name": "port",
            "title": "Port",
            "type": "number",
            "placeholder": "3306",
            'default': "3306",
            "help": "The port number of your Postgres server. This is 3306 for most users.",
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
            'type': 'text',
            "placeholder": "Database Name",
            "required": True,
        },
        {
            "name": "data_available",
            "required": True,
            "title": "Data Available",
            "type": "list",
            "values": lambda source: get_tables(source),
            "dependencies": ["host", "port", "password", "database_name"]
        }
    ],
    'categories': ['DB', 'POPULAR'],
    'keywords': ['db', 'database', 'sql'],
    'createdAt': '2020-08-02',
    'hostProperty': 'host',
    'advanced': {
        'withIncremental': True
    }
}