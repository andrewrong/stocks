import json
import logging
from datetime import timedelta, datetime
from enum import Enum
from abc import ABC, abstractmethod

import common.common
from alert.alert_func import threshold, direction_threshold
from alert.sender import Sender, TelegramSender


class ConditionType(Enum):
    Normal_Threshold = "normal_threshold"
    Direction_Threshold = "direction_threshold"  # TODO: Implement this condition


class SourceType(Enum):
    SQL = "sql"
    CONST = "const"


class Source(ABC):
    @abstractmethod
    def get_data(self, datapoint_num: int, datasource: common.common.DbClient, start: str, end: str) -> dict:
        """获取数据"""
        pass

    @abstractmethod
    def get_type(self) -> SourceType:
        """获取配置"""
        pass


class SourceSQL(Source):
    def __init__(self, sql: str, stock_symbol: str, field: str, table: str):
        self.sql = sql
        self.stock_symbol = stock_symbol
        self.field = field
        self.table = table

    def get_data(self, datapoint_num: int, datasource: common.common.DbClient, start: str, end: str) -> dict:
        # 为了保证数据能达到datapoint_num个，所以这边会外加一周的时间，取最近datapoint_num个数据
        sql = self.sql.format(self.table, self.stock_symbol, start, end)
        data = datasource.execute(sql).fetchdf()

        # 取最近datapoint_num个数据
        last_data = data[self.field][-datapoint_num:].tolist()
        return {
            'data': last_data,
            'ts': data['ts'][-datapoint_num:].tolist()
        }

    def get_type(self) -> SourceType:
        return SourceType.SQL


class SourceConst(Source):
    def __init__(self, threshold: float):
        self.threshold = threshold

    def get_data(self, datapoint_num: int, datasource: common.common.DbClient, start: str, end: str) -> dict:
        # ts 从end往前推datapoint_num个
        ts = []
        for i in range(datapoint_num):
            # ts 从end往前推datapoint_num个
            # end 从字符串变成timestamp
            ts.append((datetime.strptime(end, "%Y-%m-%d") - timedelta(days=datapoint_num - i - 1)).strftime("%Y-%m-%d"))
        return {
            'data': [self.threshold] * datapoint_num,
            'ts': ts
        }

    def get_type(self) -> SourceType:
        return SourceType.CONST


class Condition:
    def __init__(self, ctype: ConditionType, value: str, msg: str):
        self.type = ctype
        self.value = value
        self.msg = msg

    def judge(self, a: Source, b: Source, datasource: common.common.DbClient, datapoint_num: int) -> (bool, str):
        start = (datetime.now() - timedelta(days=datapoint_num + 7)).strftime("%Y-%m-%d")
        end = datetime.now().strftime("%Y-%m-%d")

        # {data, ts}
        a_data = a.get_data(datapoint_num, datasource, start, end)
        b_data = b.get_data(datapoint_num, datasource, start, end)
        b_data['ts'] = a_data['ts']

        if len(a_data['data']) != len(b_data['data']) and len(a_data['data']) != datapoint_num:
            logging.error("data length not equal, a: %d, b: %d", len(a_data['data']), len(b_data['data']))
        if len(a_data['ts']) != len(b_data['ts']) and len(a_data['ts']) != datapoint_num:
            logging.error("ts length not equal, a: %d, b: %d", len(a_data['ts']), len(b_data['ts']))

        res = False
        idx = 0

        if self.type == ConditionType.Normal_Threshold:
            res, idx = threshold(a_data['data'], b_data['data'], self.value)
        elif self.type == ConditionType.Direction_Threshold:
            res, idx = direction_threshold(a_data['data'], b_data['data'], self.value)

        if res:
            return True, self.msg.format(a_data['ts'][idx], a_data['data'][idx], b_data['data'][idx])
        else:
            return False, ''


class RuleItem:
    def __init__(self, config):
        self.datapoint_num = config.get('datapoint_num')
        self.AType = SourceType(config.get('AType'))

        if self.AType == SourceType.SQL:
            self.A = SourceSQL(config.get('A')['sql'], config.get('A')['stock'], config.get('A')['field'],
                               config.get('A')['table'])
        elif self.AType == SourceType.CONST:
            self.A = SourceConst(config.get('A')['threshold'])
        else:
            logging.fatal("Unsupported source type: %s", self.AType)

        self.BType = SourceType(config.get('BType'))
        if self.BType == SourceType.SQL:
            self.B = SourceSQL(config.get('B')['sql'], config.get('A')['stock_symbol'])
        elif self.BType == SourceType.CONST:
            self.B = SourceConst(config.get('B')['threshold'])
        else:
            logging.fatal("Unsupported source type: %s", self.BType)
        self.condition = Condition(ConditionType(config.get('condition')['type']), config.get('condition')['value'],
                                   config.get('condition')['msg'])

    def evaluate(self, data_source) -> (bool, str):
        return self.condition.judge(self.A, self.B, data_source, self.datapoint_num)


class Rule:
    def __init__(self, config, sender: Sender):
        self.name = config.get('name')
        self.rule_items = {key: RuleItem(value) for key, value in config['ruleItems'].items()}
        self.equation = config.get('equation')
        self.sender = sender

    def evaluate(self, data_source):
        item_evaluations = {key: rule_item.evaluate(data_source) for key, rule_item in self.rule_items.items()}
        result = {}
        msgs = {}

        for key, item in item_evaluations.items():
            result[key] = item[0]
            msgs[key] = item[1]

        res = eval(self.equation, {}, result)
        if res:
            msg_str = ", ".join(f"{key}: {value}" for key, value in msgs.items())
            await self.sender.send("name:{}, equation:{}, msg:{}".format(self.name, self.equation, msg_str))
        return res


class alertEngine:
    def __init__(self, config, db_client: common.common.DbClient):
        self.sender = TelegramSender(config['sender'])
        self.db_client = db_client
        self.table = config['table']
        self.cron_time = config['interval']

    async def alert(self):
        sql = "select * from {}".format(self.table)
        rules = []
        tmp = self.db_client.execute(sql).fetchall()
        for rule in tmp:
            ruleData = json.loads(rule[2])
            rules.append(Rule(ruleData, self.sender))
        for rule in rules:
            r = rule.evaluate(self.db_client)
            logging.info("name:{} judge result:{}".format(rule.name, r))
