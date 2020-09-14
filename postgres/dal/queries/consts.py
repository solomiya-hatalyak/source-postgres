DESTINATION = '{__tablename}'
BATCH_SIZE = 5000
CONNECT_TIMEOUT = 15  # seconds
MAX_RETRIES = 5
RETRY_TIMEOUT = 2

SQL_GET_ALL_TABLES = """
        SELECT * FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    """

SQL_GET_KEYS = """
            SELECT a.attname,
                   format_type(a.atttypid, a.atttypmod) as datatype,
                   i.indnatts,
                   i.indisunique,
                   i.indisprimary
            FROM   pg_index i
                   JOIN   pg_attribute a ON a.attrelid = i.indrelid
                   AND a.attnum = ANY(i.indkey)
            WHERE  i.indrelid = '{}'::regclass
            """

SQL_GET_COLUMNS = """
            SELECT a.attname,
                    a.attrelid::regclass,
                   format_type(a.atttypid, a.atttypmod) AS data_type
            FROM pg_attribute as a
            WHERE a.attrelid = '{}'::regclass
            and a.attnum > 0;
            """
