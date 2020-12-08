DESTINATION = 'postgres_{__tablename}'
BATCH_SIZE = 5000
CONNECT_TIMEOUT = 15  # seconds
MAX_RETRIES = 5
RETRY_TIMEOUT = 2

SQL_GET_ALL_TABLES = """
        SELECT * FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    """

SQL_GET_INDEXES = """
    SELECT a.attname,
            i.indexrelid,
           i.indnatts,
           i.indisunique,
           i.indisprimary,
           (SELECT i
            FROM (SELECT *,
                         row_number()
                         OVER () i
                  FROM unnest(indkey) WITH ORDINALITY AS a(att_num)) a
            WHERE att_num = attnum) as column_order
    FROM pg_index i
             JOIN pg_attribute a ON a.attrelid = i.indrelid
        AND a.attnum = ANY (i.indkey)
    WHERE i.indrelid = '{}'::regclass
    order by i.indnatts;
    """
