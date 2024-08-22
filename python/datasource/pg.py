import psycopg2
 from datetime import datetime

 class PgClient:
     def __init__(self, cfg):
         self.conn = psycopg2.connect(
             host=cfg['host'],
             port=cfg['port'],
             user=cfg['user'],
             password=cfg['password'],
             dbname=cfg['db']
         )
         self._create_table()

     def _create_table(self):
         with self.conn.cursor() as cur:
             cur.execute("""
                 CREATE TABLE IF NOT EXISTS stock_prices (
                     ts TIMESTAMP,
                     symbol VARCHAR(50),
                     open_price DOUBLE PRECISION,
                     high DOUBLE PRECISION,
                     low DOUBLE PRECISION,
                     close_price DOUBLE PRECISION,
                     volume DOUBLE PRECISION,
                     currency VARCHAR(10),
                     stock_name VARCHAR(1024),
                     stock_type VARCHAR(50),
                     PRIMARY KEY (symbol, currency, stock_name, stock_type, ts)
                 )
             """)
             self.conn.commit()

     def close(self):
         self.conn.close()

     def batch_insert(self, data):
         with self.conn.cursor() as cur:
             cur.executemany("""
                 INSERT INTO stock_prices (ts, symbol, open_price, high, low, close_price, volume, currency, stock_name, stock_type)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                 ON CONFLICT (symbol, currency, stock_name, stock_type, ts)
                 DO UPDATE SET open_price = EXCLUDED.open_price, high = EXCLUDED.high, low = EXCLUDED.low, close_price = EXCLUDED.close_price, volume =
 EXCLUDED.volume
             """, data)
             self.conn.commit()

     def type(self):
         return "postgres"