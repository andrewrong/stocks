from abc import ABC
from typing import Any, List

import duckdb
from common import common


class DuckDBClient(common.DbClient):
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
                 adj_close DOUBLE,
                 volume DOUBLE,
                 currency VARCHAR,
                 stock_name VARCHAR,
                 stock_type VARCHAR,
                 
                 sma5 DOUBLE,
                 sma20 DOUBLE,
                 sma50 DOUBLE,
                 sma120 DOUBLE,
                 sma250 DOUBLE,
                 PRIMARY KEY (symbol, currency, stock_name, stock_type, ts)
             )
         """)

    def close(self):
        self.conn.close()

    def batch_insert(self, data: List[Any]) -> None:
        try:
            with self.conn.cursor() as cur:
                cur.executemany("""
                 INSERT INTO stock_prices (ts, symbol, open_price, high, low, close_price, adj_close, volume, currency, stock_name, stock_type)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT (symbol, currency, stock_name, stock_type, ts)
                 DO UPDATE SET open_price = EXCLUDED.open_price, high = EXCLUDED.high, low = EXCLUDED.low, 
                 close_price = EXCLUDED.close_price, adj_close = EXCLUDED.adj_close, volume = EXCLUDED.volume
             """, data)
                print(f"{self.type()} batch insert success, num of data: {len(data)}")
        except duckdb.Error as e:
            print(f"batch insert error: {e}")

    def batch_update(self, data: List[Any]) -> None:

        try:
            with self.conn.cursor() as cur:
                cur.executemany("""
                 UPDATE stock_prices
                 SET sma5 = ?, sma20 = ?, sma50 = ?, sma120 = ?, sma250 = ?
                 WHERE symbol = ? AND ts = ?
             """, data)
                print(f"{self.type()} batch update success, num of data: {len(data)}")
        except duckdb.Error as e:
            print(f"batch update error: {e}")

    def execute(self, query: list) -> duckdb.DuckDBPyConnection:
        return self.conn.execute(query)

    def type(self) -> str:
        return "duckdb"
