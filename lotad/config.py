import hashlib
import os
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Optional, Union

import yaml
from dotenv import load_dotenv
from sql_metadata import Parser as SQLParser

from lotad.connection import DatabaseDetails, LotadConnectionInterface

CPU_COUNT = max(os.cpu_count() - 2, 2)


def str_presenter(dumper, data):
    """configures yaml for dumping multiline strings
    Ref: https://stackoverflow.com/questions/8640959/how-can-i-control-what-scalar-form-pyyaml-uses-for-my-data"""
    if len(data.splitlines()) > 1:  # check for multiline string
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)


yaml.add_representer(str, str_presenter)
yaml.representer.SafeRepresenter.add_representer(str, str_presenter)  # to use with safe_dum


def add_to_env(key, value, env_path):
    # Check if file exists
    file_exists = os.path.isfile(env_path)

    # Read existing content
    env_content = ""
    if file_exists:
        with open(env_path, "r") as f:
            env_content = f.read()

    # Check if key already exists
    if f"{key}=" in env_content:
        # Replace existing line
        lines = env_content.split("\n")
        for i, line in enumerate(lines):
            if line.startswith(f"{key}="):
                lines[i] = f"{key}={value}"
                break
        env_content = "\n".join(lines)
    else:
        # Add new line
        if env_content and not env_content.endswith("\n"):
            env_content += "\n"
        env_content += f"{key}={value}\n"

    # Write back to file
    with open(env_path, "w") as f:
        f.write(env_content)

    return True


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

    db1_details: DatabaseDetails
    db2_details: DatabaseDetails

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
    def db1(self) -> LotadConnectionInterface:
        return self._db1

    @property
    def db2(self) -> LotadConnectionInterface:
        return self._db2

    @property
    def _config_path(self) -> str:
        return str(os.path.dirname(self.path))

    @property
    def _env_prefix(self) -> str:
        # using the hash to prevent collisions
        # if there are multiple configs in the same directory
        return f"lotad_{hashlib.md5(self.path.encode()).hexdigest()}"

    @property
    def _db1_env_name(self):
        return f"{self._env_prefix}_password_db1"

    @property
    def _db2_env_name(self):
        return f"{self._env_prefix}_password_db2"

    def __post_init__(self):
        load_dotenv(os.path.join(self._config_path, ".env"))

        if isinstance(self.db1_details, dict):
            self.db1_details = DatabaseDetails(**self.db1_details)
            if db_password := os.getenv(self._db1_env_name):
                self.db1_details.password = db_password

        if isinstance(self.db2_details, dict):
            self.db2_details = DatabaseDetails(**self.db2_details)
            if db_password := os.getenv(self._db2_env_name):
                self.db2_details.password = db_password

        self._db1 = LotadConnectionInterface.create(self.db1_details)
        self._db2 = LotadConnectionInterface.create(self.db2_details)

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
        response["db1_details"] = self.db1_details.dict()
        response["db2_details"] = self.db2_details.dict()

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
        env_path = os.path.join(self._config_path, ".env")

        if db_password := self.db1_details.password:
            add_to_env(self._db1_env_name, db_password, env_path)
        if db_password := self.db2_details.password:
            add_to_env(self._db2_env_name, db_password, env_path)

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
