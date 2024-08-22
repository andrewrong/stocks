import duckdb


class DuckDBClient:
    def __init__(self, db_file_path):
        self.conn = duckdb.connect(database=db_file_path)
        self._create_table()

    def _create_table(self):
        self.conn.execute("""
             CREATE TABLE IF NOT EXISTS stock_prices (
                 ts TIMESTAMP,
                 symbol VARCHAR,
                 open_price DOUBLE,
                 high DOUBLE,
                 low DOUBLE,
                 close_price DOUBLE,
                 volume DOUBLE,
                 currency VARCHAR,
                 stock_name VARCHAR,
                 stock_type VARCHAR,
                 PRIMARY KEY (symbol, currency, stock_name, stock_type, ts)
             )
         """)

    def close(self):
        self.conn.close()

    def batch_insert(self, data):
        with self.conn.cursor() as cur:
            cur.executemany("""
                 INSERT INTO stock_prices (ts, symbol, open_price, high, low, close_price, volume, currency, stock_name, stock_type)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT (symbol, currency, stock_name, stock_type, ts)
                 DO UPDATE SET open_price = EXCLUDED.open_price, high = EXCLUDED.high, low = EXCLUDED.low, close_price = EXCLUDED.close_price, volume =
 EXCLUDED.volume
             """, data)

    def type(self):
        return "duckdb"
