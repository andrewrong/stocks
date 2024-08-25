import unittest

import duckdb
from compute.indicator import sma


class TestSMA(unittest.TestCase):
    def test_sma(self):
        # 模拟数据库连接
        duckConn = duckdb.connect("/Volumes/nomoshen/data/stock/stocks.db")

        # 模拟股票数据
        stock = {'symbol': 'AAPL'}

        # 测试SMA函数
        result = sma(duckConn, stock)
        print(result)


if __name__ == '__main__':
    unittest.main()