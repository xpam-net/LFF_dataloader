"""_This is the base config module._

This module is designed for processing json config files in user directory.
"""

__all__ = ['SSH', 'Base', 'Config']
__version__ = '0.2022.09.14.0'
__author__ = 'Andrey Luzhin'

from pathlib import Path
from dataclasses import dataclass, field, InitVar
from json import loads
from datetime import datetime as dt

#
# Internals
#

_PATHS_FILE: str = '~/.myconfig/paths.json'
_KEYS_FILE: str = '~/.myconfig/keys.json'
_CONNECTIONS_FILE: str = '~/.myconfig/connections.json'

#
# Public Classes
#


@dataclass
class SSH:
    host: str = '127.0.0.1'
    port: int = 22
    login: str = 'root'
    key: str | None = None  # !!! подумать про пустой relative_path
    password: str | None = field(default=None, repr=False)


    def from_dict(self,
                  dic: dict[str, int | str],
                  relative_path: Path | None = None):
        k: str
        v: int | str
        # приводим ключи к нижнему регистру,
        # т.к. внутри секций всё должно быть однозначно
        d: dict[str, int | str] = {k.lower(): v for k, v in dic.items()}
        self.host = str(d['host'])
        if d.get('port') is not None:
            self.port = int(d['port'])
        self.login = str(d['login'])
        if d.get('key') is not None:
            self.key = str(d['key'])
            if relative_path is not None:
                kf: Path = Path(self.key)
                if kf.name == str(kf):
                    # если у файла нет уже указанного пути
                    kf = relative_path / kf
                self.key = str(kf.expanduser().resolve())
        if d.get('password') is not None:
            self.password = str(d['password'])
        return self


@dataclass
class Base:
    """Class for single base representation

    equivalent to dict:

    base_dict_example: dict = {
        "ssh": {
            "host": "ssh.local",
            "port": 22,  # optional, default 22
            "login": "root",
            "key": "private.key",  # optional
            "password": "",  # optional
        },
        "engine": 'mysql+pymysql',  # optional, default 'mysql+pymysql'
        "server": "127.0.0.1",
        "port": 3306,  # optional, default 3306
        "user": "admin",
        "password": "",
        "base": "mysql"
    }
    """
    ssh: SSH | None = None
    engine: str = 'mysql+pymysql'
    server: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = field(default='', repr=False)
    base: str = 'mysql'

    def from_dict(self, dic: dict, relative_path: Path | None = None):
        k: str
        v: int | str | dict[str, int | str]
        # приводим ключи к нижнему регистру,
        # т.к. внутри секций всё должно быть однозначно
        d: dict[str, int | str] = {k.lower(): v for k, v in dic.items()}
        if 'ssh' in d.keys() and isinstance(d['ssh'], dict):
            # можно красивее if isinstance(d.get('ssh'), dict): но
            # mypy ругается, даже если ниже подставить d.get('ssh')
            self.ssh = SSH().from_dict(d['ssh'], relative_path)
        if d.get('engine') is not None:
            self.engine = str(d['engine'])
        self.server = str(d['server'])
        if d.get('port') is not None:
            self.port = int(d['port'])
        self.user = str(d['user'])
        self.password = str(d['password'])
        self.base = str(d['base'])
        return self


@dataclass
class Config:
    """_Configuration class_

    Load data from ~/.myconfig/*.json files (by default)"""
    paths_file: InitVar[Path | str | None] = None
    keys_file: InitVar[Path | str | None] = None
    connections_file: InitVar[Path | str | None] = None
    paths_path: Path = field(init=False)
    keys_path: Path = field(init=False)
    connections_path: Path = field(init=False)
    paths: dict[str, Path] = field(init=False, repr=False)
    keys: dict[str, str] = field(init=False, repr=False)
    connections: dict[str, dict[str, Base]] = field(init=False, repr=False)

    def __post_init__(self, paths_file, keys_file, connections_file) -> None:
        self.load_paths_file(paths_file, transform_connections=False)
        self.load_keys_file(keys_file)
        self.load_connections_file(connections_file)

    def load_paths_file(self,
                        file: Path | str | None = None,
                        transform_connections: bool = True) -> None:
        self.paths_path = Path(_PATHS_FILE) if file is None else Path(file)
        self.paths_path = self.paths_path.expanduser().resolve()
        if self.paths_path.is_file():
            json: dict = loads(self.paths_path.read_text())
            self.paths = {}
            k: str
            v: str
            for k, v in json.items():
                self.paths[k] = Path(v).expanduser().resolve()
        if transform_connections:
            self.load_connections_file(self.connections_path)

    def load_keys_file(self, file: Path | str | None = None) -> None:
        self.keys_path = Path(_KEYS_FILE) if file is None else Path(file)
        self.keys_path = self.keys_path.expanduser().resolve()
        if self.keys_path.is_file():
            json: dict = loads(self.keys_path.read_text())
            self.keys = {}
            k: str
            v: str
            for k, v in json.items():
                self.keys[k] = v

    def load_connections_file(self, file: Path | str | None = None) -> None:
        self.connections_path = Path(
            _CONNECTIONS_FILE) if file is None else Path(file)
        self.connections_path = self.connections_path.expanduser().resolve()
        if self.connections_path.is_file():
            json: dict = loads(self.connections_path.read_text())
            self.connections = {}
            section_key: str
            section_value: dict[str, dict[str,
                                          int | str | dict[str, int | str]]]
            for section_key, section_value in json.items():
                bases: dict[str, Base] = {}
                base_name: str
                base_dict: dict[str, int | str | dict[str, int | str]]
                for base_name, base_dict in section_value.items():
                    bases[base_name] = Base().from_dict(
                        base_dict, self.paths['ssh'])
                self.connections[section_key] = bases

    def make_filename(self,
                      name: str | Path,
                      in_path: str = 'export',
                      date: bool = True,
                      time: bool = True) -> str:
        file_name: Path = Path(name)
        name_extra: str = ''
        if date:
            name_extra += dt.today().strftime(r"_%Y%m%d")
        if time:
            name_extra += dt.today().strftime(r"_%H%M%S")
        extensions: str = ''.join(file_name.suffixes)
        file_path: Path = self.paths[in_path] if file_name.name == str(
            name) else file_name.parent
        file_name = file_path.expanduser().resolve() / Path(
            file_name.name.removesuffix(extensions) + name_extra + extensions)
        return str(file_name)


if __name__ == "__main__":
    cfg = Config()

    # print(cfg.make_filename('test.xlsx'))

    # print(cfg.connections)

    # q: Base = cfg.connections['replica']['qp']
    # if q.ssh is not None:
    #     print(q.ssh.key_file)