## 1. 报警的设计

在股票市场中，基本上的报警都是阈值报警，或者有方向性的阈值告警；还有就是曲线的上穿或者下穿类似的报警；我本来思考是用现成的开源的，但是现成的都都有点
复杂的，不然就是要使用时序数据库，和我目前的选择pg或duckdb的使用有点不一样。所以就想着自己写一个。

## 2. 协议思路

* rule: 表示整体的报警规则
* ruleItem: 一条具体的报警逻辑判断的规则，会包含具体的报警条件 + 数据源什么的
* []ruleItem: 多条报警逻辑判断的规则
* A  && B || C: 一整套逻辑的结合

## 3. 协议实现

* ruleItem:
```json
{
  "datapoint_num": 3,
  "AType": "sql",
  "A": {
    "sql": "select * from tableA where a = 1",
    "stock": "stock_symbol"
  },
  "BType": "const",
  "B": {
    "threshold": 0.5
  },
  "condition": {
    "type": "normal_threshold",
    "value": "A < B",
    // "type": "direction_threshold",
    // "value": "A up B" or "A down B",
    "msg": "A 上传 B，时间: ts, A: value, B: value"
  }
}
```
* rule:
```json
{
  "name": "test",
  "ruleItems": {
    "A": {
      "datapoint_num": 3,
      "AType": "sql",
      "A": {
        "sql": "select * from tableA where a = 1",
        "stock": "stock_symbol",
        "field": "rsi17",
        "table": "stock_prices_2"
      },
      "BType": "const",
      "B": {
        "threshold": 0.5
      },
      "condition": {
        "type": "normal_threshold",
        "value": "A < B"
        // "type": "direction_threshold",
        // "value": "A up B" or "A down B",
      },
      "msg": "A 上传 B，时间: ts, A: value, B: value"
    },
    "B": {
      "datapoint_num": 3,
      "AType": "sql",
      "A": {
        "sql": "select * from tableA where a = 1",
        "stock": "stock_symbol"
      },
      "BType": "const",
      "B": {
        "threshold": 0.5
      },
      "condition": {
        "type": "normal_threshold",
        "value": "A < B"
        // "type": "direction_threshold",
        // "value": "A up B" or "A down B",
      },
      "msg": "A 上传 B，时间: ts, A: value, B: value"
    }
  },
  "equation": "A && B || C"
}
```
