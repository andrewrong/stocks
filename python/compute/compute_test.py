import unittest

import duckdb

import common.common
from compute.indicator import calc_multi_indicator
from compute.indicator import macd
import matplotlib.pyplot as plt


class TestSMA(unittest.TestCase):
    def test_sma(self):
        # 模拟数据库连接
        duckConn = duckdb.connect("/Volumes/nomoshen/data/stock/stocks_2.db")

        # 模拟股票数据
        stock = common.common.StockInfo("AAPL", "Apple Inc.", common.common.StockType.STOCK, "USD")

        # 测试SMA函数
        result = calc_multi_indicator(duckConn, stock, "2022-01-01", "2022-02-01")
        print(result)
    def test_macd(self):
        # 模拟数据库连接
        duckConn = duckdb.connect("/Volumes/nomoshen/data/stock/stocks_2.db")

        # 模拟股票数据
        stock = common.common.StockInfo("AAPL", "Apple Inc.", common.common.StockType.STOCK, "USD")

        query = "select ts, adj_close from stock_prices where symbol = '{}' " \
                "and adj_close is not null and adj_close != 0 and ts >= '{}' and ts <= '{}' order by ts".format(
            stock.symbol, "2024-05-01", "2024-08-26")
        dataframes = duckConn.execute(query).fetchdf()

        # 测试MACD函数
        data = macd(dataframes)

        for index, ts in data['ts'].items():
            print(ts, data["macd"]["macd"][index], data["macd"]["macd_signal"][index], data["macd"]["macd_hist"][index])

        # 绘制结果
        plt.figure(figsize=(14, 7))
        # 绘制收盘价
        plt.subplot(2, 1, 1)
        plt.plot(data['ts'], dataframes["adj_close"],  label='Close Price')
        plt.title('Stock Price and MACD')
        plt.legend()
        # 绘制MACD和信号线
        plt.subplot(2, 1, 2)
        plt.plot(data['ts'], data["macd"]['macd'], label='MACD', color='blue')
        plt.plot(data['ts'], data["macd"]['macd_signal'], label='Signal Line', color='red')
        plt.bar(data['ts'], data['macd']['macd_hist'], label='MACD Histogram', color='gray')
        plt.legend()
        plt.show()


if __name__ == '__main__':
    unittest.main()