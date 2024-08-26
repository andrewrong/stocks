from datetime import datetime, timedelta

from talib import abstract

import common.common

SMA = abstract.Function("sma")
BBANDS = abstract.Function("bbands")
EMA = abstract.Function("ema")
MACD = abstract.Function("macd")
RSI = abstract.Function("rsi")


def sma(dataframes: dict, period=30) -> dict:
    """
    Returns:
        dict: {"sma": list of float, "ts": list of timestamps}
    """
    return {"sma": SMA(dataframes['adj_close'], timeperiod=period), "ts": dataframes['ts']}


def multi_sma(dataframes: dict, periods=None) -> dict:
    """
    Returns:
        dict: {"sma": dict of lists of float, "ts": list of timestamps}
    """
    if periods is None:
        periods = [5, 20, 50, 120, 200]
    res = {f"sma{period}": SMA(dataframes['adj_close'], timeperiod=period) for period in periods}
    return {"sma": res, "ts": dataframes['ts']}


def sma5(dataframes: dict) -> dict:
    """
    Returns:
        dict: {"sma": list of float, "ts": list of timestamps}
    """
    return sma(dataframes, 5)


def sma20(dataframes: dict) -> dict:
    """
    Returns:
        dict: {"sma": list of float, "ts": list of timestamps}
    """
    return sma(dataframes, 20)


def sma50(dataframes: dict) -> dict:
    """
    Returns:
        dict: {"sma": list of float, "ts": list of timestamps}
    """
    return sma(dataframes, 50)


def sma120(dataframes: dict) -> dict:
    """
    Returns:
        dict: {"sma": list of float, "ts": list of timestamps}
    """
    return sma(dataframes, 120)


def sma200(dataframes: dict) -> dict:
    """
    Returns:
        dict: {"sma": list of float, "ts": list of timestamps}
    """
    return sma(dataframes, 200)


# 这个指标：Bollinger Bands， 用来检测股票价格的离散程度， 与其它的技术指标不同的是，它不是基于价格的，而是基于价格的波动。
def bbands(dataframes: dict, period=20, nbdevup=2, nbdevdn=2, matype=0) -> dict:
    """
    Returns:
        dict: {"bbands": {"upper": list of float, "middle": list of float, "lower": list of float}, "ts": list of timestamps}
    """
    upper, middle, lower = BBANDS(dataframes['adj_close'], timeperiod=period, nbdevup=nbdevup, nbdevdn=nbdevdn,
                                  matype=matype)
    return {"bbands": {"upper": upper, "middle": middle, "lower": lower}, "ts": dataframes['ts']}


# 新增的 ema 函数, 这个指标类似于sma，不同的在于对最近的点的权重会更加大一些，会更加敏感，适合做短期交易和趋势分析
def ema(dataframes: dict, period=30) -> dict:
    """
    Returns:
        dict: {"ema": list of float, "ts": list of timestamps}
    """
    return {"ema": EMA(dataframes['adj_close'], timeperiod=period), "ts": dataframes['ts']}


# 不同周期的 ema 函数
def multi_ema(dataframes: dict, periods=None) -> dict:
    """
    Returns:
        dict: {"ema": dict of lists of float, "ts": list of timestamps}
    """
    if periods is None:
        periods = [5, 20, 50, 120, 200]
    res = {f"ema{period}": EMA(dataframes['adj_close'], timeperiod=period) for period in periods}
    return {"ema": res, "ts": dataframes['ts']}


def ema5(dataframes: dict) -> dict:
    """
    Returns:
        dict: {"ema": list of float, "ts": list of timestamps}
    """
    return ema(dataframes, 5)


def ema20(dataframes: dict) -> dict:
    """
    Returns:
        dict: {"ema": list of float, "ts": list of timestamps}
    """
    return ema(dataframes, 20)


def ema50(dataframes: dict) -> dict:
    """
    Returns:
        dict: {"ema": list of float, "ts": list of timestamps}
    """
    return ema(dataframes, 50)


def ema120(dataframes: dict) -> dict:
    """
    Returns:
        dict: {"ema": list of float, "ts": list of timestamps}
    """
    return ema(dataframes, 120)


def ema200(dataframes: dict) -> dict:
    """
    Returns:
        dict: {"ema": list of float, "ts": list of timestamps}
    """
    return ema(dataframes, 200)


'''
### MACD (Moving Average Convergence/Divergence) - 移动平均线收敛/发散指标

MACD 是一种广泛使用的技术分析指标，由 Gerald Appel 于 1970 年代开发，用于识别价格趋势的变化、动量的方向以及潜在的买入和卖出信号。MACD 主要由三部分组成：MACD 线、信号线和柱状图。

### 组成部分

1. **MACD 线**：
   - 计算公式：`MACD 线 = 12日 EMA（快速） - 26日 EMA（慢速）`
   - 这是两个不同周期的指数移动平均线之间的差异。

2. **信号线**：
   - 计算公式：`信号线 = 9日 EMA（MACD 线）`
   - 这是 MACD 线的 9 日指数移动平均线，用于平滑 MACD 线。

3. **柱状图**：
   - 计算公式：`柱状图 = MACD 线 - 信号线`
   - 柱状图表示 MACD 线和信号线之间的差异，用于判断趋势的强弱。

### 使用方法

1. **交叉信号**：
   - **买入信号**：当 MACD 线从下方上穿信号线时，表示价格可能开始上涨，是一个买入信号。
   - **卖出信号**：当 MACD 线从上方下穿信号线时，表示价格可能开始下跌，是一个卖出信号。

2. **柱状图**：
   - 柱状图的正负值和高度表示 MACD 线和信号线之间的距离，可以用来判断趋势的强弱。
   - 当柱状图从负值转为正值，表示动量转为向上；反之亦然。

3. **零线**：
   - 当 MACD 线和信号线都在零线之上时，表示市场处于多头趋势；反之，则表示市场处于空头趋势。

### 参数

- **fastperiod（快速周期）**：通常为 12，表示快速指数移动平均线的周期。
- **slowperiod（慢速周期）**：通常为 26，表示慢速指数移动平均线的周期。
- **signalperiod（信号周期）**：通常为 9，表示信号线的周期。

### 示例

假设我们使用默认参数（12, 26, 9），我们可以通过以下步骤计算 MACD：
1. **计算 12 日 EMA（快速线）**。
2. **计算 26 日 EMA（慢速线）**。
3. **计算 MACD 线**：
   \[
   \text{MACD 线} = 12 \text{日 EMA} - 26 \text{日 EMA}
   \]
4. **计算 9 日 EMA（信号线）**：
   \[
   \text{信号线} = 9 \text{日 EMA（MACD 线）}
   \]
5. **计算柱状图**：
   \[
   \text{柱状图} = \text{MACD 线} - \text{信号线}
   \]

通过以上步骤，分析师可以监控 MACD 线的交叉情况和柱状图的变化来做出交易决策。
'''


def macd(dataframes: dict) -> dict:
    macd, macd_signal, macd_hist = MACD(dataframes['adj_close'])
    return {"macd": {"macd": macd, "macd_signal": macd_signal, "macd_hist": macd_hist}, "ts": dataframes['ts']}


'''
RSI（Relative Strength Index，相对强弱指标）是由 J. Welles Wilder 于1978年提出的一种动量振荡指标，用于衡量价格上涨和下跌的强度。RSI 的数值范围是 0 到 100，通常用于识别超买和超卖的市场状态。

### RSI 的计算逻辑

RSI 的计算分为以下几个步骤：

1. **计算单周期收益（Gain）和损失（Loss）**：
   - **收益（Gain）**：如果当天的收盘价高于前一天的收盘价，则收益为正，否则为 0。
   - **损失（Loss）**：如果当天的收盘价低于前一天的收盘价，则损失为正，否则为 0。

2. **计算平均收益（Average Gain）和平均损失（Average Loss）**：
   对于给定周期（通常为 14 天）计算平均收益和平均损失：
   \[
   \text{Average Gain} = \frac{\sum_{i=1}^{N} \text{Gain}_i}{N}
   \]
   \[
   \text{Average Loss} = \frac{\sum_{i=1}^{N} \text{Loss}_i}{N}
   \]

3. **计算相对强弱（Relative Strength, RS）**：
   \[
   \text{RS} = \frac{\text{Average Gain}}{\text{Average Loss}}
   \]

4. **计算 RSI**：
   \[
   \text{RSI} = 100 - \frac{100}{1 + \text{RS}}
   \]

### RSI 的应用

1. **超买和超卖信号**：
   - **超买**：当 RSI 高于 70 时，市场可能处于超买状态，价格可能会回调。
   - **超卖**：当 RSI 低于 30 时，市场可能处于超卖状态，价格可能会反弹。

2. **趋势信号**：
   - **RSI 上穿**：当 RSI 从下方上穿 30 或 50 时，可能是买入信号。
   - **RSI 下穿**：当 RSI 从上方下穿 70 或 50 时，可能是卖出信号。

### 为什么 RSI 能衡量价格上涨和下跌的强度

1. **相对强弱**：RSI 通过比较一段时间内的平均收益和平均损失，衡量当前价格的强弱。当平均收益较高且平均损失较低时，RSI 值较高，反之亦然。
   
2. **动量振荡**：RSI 作为一种动量振荡指标，可以反映市场的动量变化。高 RSI 值表示市场有较强的上升动量，而低 RSI 值表示市场有较强的下降动量。

3. **标准化范围**：RSI 的值被规范化到 0 到 100 的范围内，使得交易者可以直观地识别何时市场可能过度买入或卖出，从而做出相应的交易决策。
'''


def rsi(dataframes: dict, period=14) -> dict:
    """
    Returns:
        dict: {"rsi": list of float, "ts": list of timestamps}
    """
    rsi = RSI(dataframes['adj_close'], timeperiod=period)
    return {"rsi": rsi, "ts": dataframes['ts']}


def calc_multi_indicator(client: common.common.DbClient, stock: common.common.StockInfo, start: str, end: str,
                         periods=None) -> dict:
    if periods is None:
        periods = [5, 20, 50, 120, 200]
    # 如果start 不能大于end - max(periods)，就用end - max(periods)作为start
    # 将日期字符串转换为 datetime 对象
    start_dt = datetime.strptime(start, '%Y-%m-%d')
    end_dt = datetime.strptime(end, '%Y-%m-%d')
    # 计算最大的时间间隔
    max_period = max(periods)
    # 如果 start 不能大于 end - max_period，就用 end - max_period 作为 start
    if start_dt > end_dt - timedelta(days=max_period):
        start_dt = end_dt - timedelta(days=max_period)
    # 将 datetime 对象转换为字符串
    start = start_dt.strftime('%Y-%m-%d')
    end = end_dt.strftime('%Y-%m-%d')

    query = "select ts, adj_close from stock_prices where symbol = '{}' " \
            "and adj_close is not null and adj_close != 0 and ts >= '{}' and ts <= '{}' order by ts".format(
        stock.symbol, start, end)
    dataframes = client.execute(query).fetchdf()

    results = {}

    m_sma = multi_sma(dataframes, periods)
    m_ema = multi_ema(dataframes, periods)
    rsi_data = rsi(dataframes)
    macd_data = macd(dataframes)
    bbands_data = bbands(dataframes)

    # 过滤数据，确保返回的数据仅包含在指定区间内的数据点
    filtered_ts = dataframes['ts'][(dataframes['ts'] >= start) & (dataframes['ts'] <= end)]
    filtered_indices = filtered_ts.index

    results['sma'] = {key: value[filtered_indices] for key, value in m_sma['sma'].items()}
    results['ema'] = {key: value[filtered_indices] for key, value in m_ema['ema'].items()}
    results['rsi'] = rsi_data['rsi'][filtered_indices]
    results['macd'] = {key: value[filtered_indices] for key, value in macd_data['macd'].items()}
    results['bbands'] = {key: value[filtered_indices] for key, value in bbands_data['bbands'].items()}
    results["ts"] = filtered_ts

    return results
