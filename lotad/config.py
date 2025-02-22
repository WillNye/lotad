import os
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Optional, Union

import yaml
from sql_metadata import Parser as SQLParser

from lotad.connection import LotadConnectionInterface

CPU_COUNT = max(os.cpu_count() - 2, 2)


def str_presenter(dumper, data):
    """configures yaml for dumping multiline strings
    Ref: https://stackoverflow.com/questions/8640959/how-can-i-control-what-scalar-form-pyyaml-uses-for-my-data"""
    if len(data.splitlines()) > 1:  # check for multiline string
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)


yaml.add_representer(str, str_presenter)
yaml.representer.SafeRepresenter.add_representer(str, str_presenter) # to use with safe_dum


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
class TableConfig:
    table_name: str
    _rules: Optional[list[TableRule]] = None
    _query: Optional[str] = None

    _rule_map: dict[str, TableRule] = None

    def __init__(
        self, 
        table_name: str, 
        rules: Optional[list[TableRule]] = None, 
        query: Optional[str] = None
    ):
        self.table_name = table_name
        self.rules = rules or []
        self.query = query

    def dict(self):
        response = {'table_name': self.table_name}
        if self._query:
            response['query'] = self._query
        if self._rules:
            response['rules'] = sorted(
                [rule.dict() for rule in self._rules],
                key=lambda x: f"{x['rule_type']}:{x['rule_value']}"
            )
        return response
    
    @property
    def rules(self) -> list[TableRule]:
        return self._rules
    
    @rules.setter
    def rules(self, rules: list[Union[TableRule, dict]]):
        self._rules = [
            r if isinstance(r, TableRule) else TableRule(**r) 
            for r in rules
        ]
        self._rule_map = {
            table_rule.rule_value: table_rule
            for table_rule in self._rules
        }
    
    def add_rule(self, rule: TableRule):
        self._rule_map[rule.rule_value] = rule
        self.rules = list(self._rule_map.values())

    def get_rule(self, rule_value: str) -> Union[TableRule, None]:
        return self._rule_map.get(rule_value)
    
    @property
    def query(self) -> Optional[str]:
        if not self._query:
            return None

        return self._query
    
    @query.setter
    def query(self, query: Optional[str]):               
        if not query:
            return

        # Check for CTEs
        if query.lower().startswith("with"):
            raise ValueError("CTEs are not currently supported")
        
        try:
            SQLParser(query)
        except Exception as e:
            raise ValueError("Unable to parse query")

        # Remove any extra new lines and whitespace
        # Required for the yaml dump to work
        split_query = query.split("\n")        
        self._query = "\n".join(
            q_line.lstrip(" ").rstrip(" ") 
            for q_line in split_query if q_line.strip(" ")
            )
        if not self._query.endswith(";"):
            self._query += ";"


@dataclass
class Config:
    path: str

    db1_connection_string: str
    db2_connection_string: str

    output_path: str = 'drift_analysis.db'

    target_tables: Optional[list[str]] = None
    ignore_tables: Optional[list[str]] = None

    table_configs: Optional[list[TableConfig]] = None

    ignore_dates: bool = False

    _table_configs_map: dict[str, TableConfig] = None

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

        if not self.ignore_tables:
            self.ignore_tables = []
        if not self.target_tables:
            self.target_tables = []

        if self.table_configs:
            for i, table_rule in enumerate(self.table_configs):
                if isinstance(table_rule, dict):
                    self.table_configs[i] = TableConfig(**table_rule)

            self._table_configs_map = {
                table_configs.table_name: table_configs
                for table_configs in self.table_configs
            }
        else:
            self._table_configs_map = {}

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

        if "table_configs" in response:
            response['table_configs'] = sorted(
                [tr.dict() for tr in self.table_configs],
                key=lambda x: x['table_name']
            )

        return response

    def write(self):
        config_dict = self.dict()
        with open(self.path, 'w') as f:
            yaml.dump(config_dict, f, indent=2)

    def update_table_config(
        self, 
        table: str, 
        table_rule: Optional[TableRule] = None, 
        query: Optional[str] = None
    ):
        if not table_rule and not query:
            raise ValueError("table_rule or query must be provided")

        if table not in self._table_configs_map:
            self._table_configs_map[table] = TableConfig(table)

        if table_rule:
            self._table_configs_map[table].add_rule(table_rule)
        if query:
            self._table_configs_map[table].query = query

        self.table_configs = list(self._table_configs_map.values())

    def get_table_config(self, table: str) -> Union[TableConfig, None]:
        return self._table_configs_map.get(table)
