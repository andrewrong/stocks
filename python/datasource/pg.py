import psycopg2
from common import common


class PgClient(common.DbClient):
    def __init__(self, cfg):
        self.conn = psycopg2.connect(
            host=cfg['host'],
            port=cfg['port'],
            user=cfg['user'],
            password=cfg['pass'],
            dbname=cfg['db']
        )
        self._create_table()

    def _create_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                 CREATE TABLE IF NOT EXISTS stock_prices_2 (
                     ts TIMESTAMP,
                     symbol VARCHAR(50),
                     open_price DOUBLE PRECISION,
                     high DOUBLE PRECISION,
                     low DOUBLE PRECISION,
                     close_price DOUBLE PRECISION,
                     adj_close DOUBLE PRECISION,
                     volume DOUBLE PRECISION,
                     currency VARCHAR(10),
                     stock_name VARCHAR(1024),
                     stock_type VARCHAR(50),
                     
                     sma5 DOUBLE PRECISION,
                     sma20 DOUBLE PRECISION,
                     sma50 DOUBLE PRECISION,
                     sma120 DOUBLE PRECISION,
                     sma200 DOUBLE PRECISION,
                     
                     ema5 DOUBLE PRECISION,
                     ema20 DOUBLE PRECISION,
                     ema50 DOUBLE PRECISION,
                     ema120 DOUBLE PRECISION,
                     ema200 DOUBLE PRECISION,
                     
                     macd DOUBLE PRECISION,
                     macd_signal DOUBLE PRECISION,
                     macd_hist DOUBLE PRECISION,
                     rsi7 DOUBLE PRECISION,
                     rsi14 DOUBLE PRECISION,
                     rsi28 DOUBLE PRECISION,
                 
                     bb_upper DOUBLE PRECISION,
                     bb_middle DOUBLE PRECISION,
                     bb_lower DOUBLE PRECISION,
                     
                     PRIMARY KEY (symbol, currency, stock_name, stock_type, ts)
                 )
             """)
            self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def batch_insert_stockinfo(self, data: list) -> None:
        pass

    def batch_insert(self, data: list) -> None:
        try:
            with self.conn.cursor() as cur:
                cur.executemany("""
                 INSERT INTO stock_prices_2 (ts, symbol, open_price, high, low, close_price, adj_close, volume, currency, stock_name, stock_type, 
                 sma5, sma20, sma50, sma120, sma200, ema5, ema20, ema50, ema120, ema200, macd, macd_signal, macd_hist, rsi7, rsi14, rsi28, bb_upper, bb_middle, bb_lower)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                 ON CONFLICT (symbol, currency, stock_name, stock_type, ts)
                 DO UPDATE SET open_price = EXCLUDED.open_price, high = EXCLUDED.high, low = EXCLUDED.low, close_price = EXCLUDED.close_price, volume =
 EXCLUDED.volume, adj_close = EXCLUDED.adj_close, sma5 = EXCLUDED.sma5, sma20 = EXCLUDED.sma20, sma50 = EXCLUDED.sma50, sma120 = EXCLUDED.sma120, sma200 = EXCLUDED.sma200,
 ema5 = EXCLUDED.ema5, ema20 = EXCLUDED.ema20, ema50 = EXCLUDED.ema50, ema120 = EXCLUDED.ema120, ema200 = EXCLUDED.ema200, macd = EXCLUDED.macd, macd_signal = EXCLUDED.macd_signal,
 macd_hist = EXCLUDED.macd_hist, rsi7 = EXCLUDED.rsi7, rsi14 = EXCLUDED.rsi14, rsi28 = EXCLUDED.rsi28, bb_upper = EXCLUDED.bb_upper, bb_middle = EXCLUDED.bb_middle, bb_lower = EXCLUDED.bb_lower
             """, data)
                self.conn.commit()
                print(f"{self.type()} batch insert success, num of data: {len(data)}")
        except psycopg2.Error as e:
            print(f"batch insert error: {e}")

    def batch_update(self, data: list) -> None:

        try:
            with self.conn.cursor() as cur:
                cur.executemany("""
                 UPDATE stock_prices_2
                 SET sma5 = ?, sma20 = ?, sma50 = ?, sma120 = ?, sma200 = ?, ema5 = ?, ema20 = ?, ema50 = ?, ema120 = ?, 
                 ema200 = ?, macd = ?, macd_signal = ?, macd_hist = ?, bb_upper = ?, bb_middle = ?, bb_lower = ?,
                 rsi7 = ?, rsi14 = ?, rsi28 = ?
                 WHERE symbol = %s AND ts = %s
             """, data)
                self.conn.commit()
                print(f"{self.type()} batch update success, num of data: {len(data)}")
        except psycopg2.Error as e:
            print(f"batch update error: {e}")

    def execute(self, query: str):

        try:
            with self.conn.cursor() as cur:
                return cur.execute(query)
        except psycopg2.Error as e:
            print(f"pg execute error: {e}")
            return None

    def type(self) -> str:
        return "postgres"
