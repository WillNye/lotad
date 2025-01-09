import os
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Union

import yaml

from lotad.connection import LotadConnectionInterface

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
            'rules': sorted(
                [rule.dict() for rule in self.rules],
                key=lambda x: f"{x['rule_type']}:{x['rule_value']}"
            ),
        }

    def get_rule(self, rule_value: str) -> Union[TableRule, None]:
        return self._rule_map.get(rule_value)


@dataclass
class Config:
    path: str

    db1_connection_string: str
    db2_connection_string: str

    output_path: str = 'drift_analysis.db'

    target_tables: list[str] = None
    ignore_tables: list[str] = None

    table_rules: list[TableRules] = None

    ignore_dates: bool = False

    _table_rules_map: dict[str, TableRules] = None

    _db1: LotadConnectionInterface = None
    _db2: LotadConnectionInterface = None

    # Any attr that starts with an underscore is not versioned by default
    _unversioned_config_attrs = ["path"]

    @classmethod
    def load(cls, path):
        with open(path, 'r') as f:
            config_dict = yaml.safe_load(f)
            return Config(path=path, **config_dict)

    @property
    def db1(self):
        return self._db1

    @property
    def db2(self):
        return self._db2

    def __post_init__(self):
        self._db1 = LotadConnectionInterface.create(self.db1_connection_string)
        self._db2 = LotadConnectionInterface.create(self.db2_connection_string)

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
        response = {
            k: v
            for k, v in asdict(self).items()
            if v and not (k in self._unversioned_config_attrs or k.startswith('_'))
        }

        if "target_tables" in response:
            response["target_tables"] = sorted(response["target_tables"])

        if "ignore_tables" in response:
            response["ignore_tables"] = sorted(response["ignore_tables"])

        if "table_rules" in response:
            response['table_rules'] = sorted(
                [tr.dict() for tr in self.table_rules],
                key=lambda x: x['table_name']
            )

        return response

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
