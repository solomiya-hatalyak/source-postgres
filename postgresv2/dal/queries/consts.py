DESTINATION = 'postgres_{__tablename}'
BATCH_SIZE = 5000
CONNECT_TIMEOUT = 15  # seconds
MAX_RETRIES = 5
RETRY_TIMEOUT = 2

SQL_GET_ALL_TABLES = """
        SELECT * FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    """
