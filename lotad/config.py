import os
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Union

import yaml

CPU_COUNT = max(os.cpu_count() - 2, 2)


class TableRuleType(Enum):
    IGNORE_COLUMN = 'ignore_column'


@dataclass
class TableRule:
    rule_type: TableRuleType
    rule_value: str

    def __init__(self, rule_type: TableRuleType, rule_value: str):
        if isinstance(rule_type, str):
            rule_type = TableRuleType(rule_type)

        self.rule_type = rule_type
        self.rule_value = rule_value

    def dict(self):
        return {
            'rule_type': self.rule_type.value,
            'rule_value': self.rule_value,
        }


@dataclass
class TableRules:
    table_name: str
    rules: list[TableRule]

    _rule_map: dict[str, TableRule] = None

    def __post_init__(self):

        for i, rule in enumerate(self.rules):
            if isinstance(rule, dict):
                self.rules[i] = TableRule(**rule)

        self._rule_map = {
            table_rule.rule_value: table_rule
            for table_rule in self.rules
        }

    def dict(self):
        return {
            'table_name': self.table_name,
            'rules': [rule.dict() for rule in self.rules],
        }

    def get_rule(self, rule_value: str) -> Union[TableRule, None]:
        return self._rule_map.get(rule_value)


@dataclass
class Config:
    path: str

    ignore_dates: bool

    db1_connection_string: str
    db2_connection_string: str

    target_tables: list[str] = None
    ignore_tables: list[str] = None

    table_rules: list[TableRules] = None
    _table_rules_map: dict[str, TableRules] = None

    @classmethod
    def load(cls, path):
        with open(path, 'r') as f:
            config_dict = yaml.safe_load(f)
            return Config(path=path, **config_dict)

    def __post_init__(self):
        if self.table_rules:
            for i, table_rule in enumerate(self.table_rules):
                if isinstance(table_rule, dict):
                    self.table_rules[i] = TableRules(**table_rule)

            self._table_rules_map = {
                table_rules.table_name: table_rules
                for table_rules in self.table_rules
            }
        else:
            self._table_rules_map = {}

    def dict(self):
        response = asdict(self)
        del response['path']
        del response['_table_rules_map']
        response['table_rules'] = [tr.dict() for tr in self.table_rules]

        return {k: v for k, v in response.items() if v}

    def write(self):
        config_dict = self.dict()
        with open(self.path, 'w') as f:
            yaml.dump(config_dict, f)

    def add_table_rule(self, table, rule_type: TableRuleType, rule_value: str):
        if table in self._table_rules_map:
            self._table_rules_map[table].rules.append(
                TableRule(rule_type, rule_value)
            )
        else:
            self._table_rules_map[table] = TableRules(
                table,
                [TableRule(rule_type, rule_value)]
            )

        self.table_rules = list(self._table_rules_map.values())

    def get_table_rules(self, table: str) -> Union[TableRules, None]:
        return self._table_rules_map.get(table)
