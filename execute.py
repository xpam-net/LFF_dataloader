"""This is database connection module

This module for connecting to multiple databases and retrieving
data as a DataFrame
"""

__all__ = ['execute_on_base', 'get_dict_of_bases', 'execute']
__version__ = '0.2022.09.16.0'
__author__ = 'Andrey Luzhin'

import pandas as pd
#import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sshtunnel import SSHTunnelForwarder
from dataloader.config import Config, Base
from dataloader.dicts import bases as base_names
from collections.abc import Iterable
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.cursor import CursorResult

#
# Public functions
#


def execute_on_base(sql: str,
                    base: Base | dict,
                    title: str | None = None,
                    debug: bool = True,
                    **kwargs) -> pd.DataFrame | None:
    """Execute request on a single base

    Arguments:
        sql {str} -- SQL request
        base {Base | dict} -- base

    Keyword Arguments:
        title {str | None} -- title to show (default: {None})
        debug {bool} -- print debug information (default: {True})

    Returns:
        pd.DataFrame | None -- DataFrame if data exists
    """
    b = base if isinstance(base, Base) else Base().from_dict(base)
    tunnel: SSHTunnelForwarder | None = None
    if b.ssh is not None:
        tunnel = SSHTunnelForwarder(
            (b.ssh.host, b.ssh.port),
            ssh_username=b.ssh.login,
            ssh_pkey=b.ssh.key,
            ssh_password=b.ssh.password,
            allow_agent=False,  # disable id_rsa search
            remote_bind_address=(b.server, b.port))
    if tunnel is not None:
        tunnel.start()
    engine: Engine = create_engine(
        URL.create(drivername=b.engine,
                   username=b.user,
                   password=b.password,
                   host=b.server,
                   port=b.port if tunnel is None else tunnel.local_bind_port,
                   database=b.base),
        connect_args={'client_flag': 0}
        if b.engine.startswith('mysql') else {}
        # return affected rowcount, not filtered with WHERE rouwcount
    )
    # print(engine.dialect.driver, engine.name)
    if debug:
        print(f'Connected {title}')
    data: pd.DataFrame | None = None

    with engine.connect() as connection:
        #data: pd.DataFrame | None = pd.read_sql(sql, connection)
        result: CursorResult = connection.execute(sql)
        if result.returns_rows:  # and result.rowcount > 0: # !!! для select rowcount может не быть
            #df = pd.DataFrame(result.fetchall())
            #df.columns = result.keys()
            data = pd.DataFrame.from_records(result.fetchall(),
                                             columns=result.keys())
        else:
            print(f'Affected rows - {result.rowcount}')

    if debug:
        print(f'{title} done {"(empty)" if data is None else data.shape}')
    if tunnel is not None:
        tunnel.stop()
        # tunnel = None
    return data


def get_dict_of_bases(config: Config | None = None,
                      instance: str = 'replica',
                      bases: str | Iterable[str] | dict[str, str | Base]
                      | None = None,
                      debug: bool = True) -> dict[str, Base]:

    def _gen_dict(connections: dict[str, Base],
                  base: str,
                  title: str | None = None,
                  debug: bool = True) -> dict[str, Base]:
        """Service function for proper dict generation

        Arguments:
            connections {dict[str, Base]} -- dict of connections
            base {str} -- base name

        Keyword Arguments:
            title {str | None} -- base title. If None, will search in
                global dict (default: {None})
            debug {bool} -- to print debug info or not (default: {True})

        Returns:
            dict[str, Base] -- resulting dict
        """
        if connections.get(base) is not None:
            return {
                title if title is not None else base_names.get(base, base):
                connections[base]
            }
        else:
            if debug:
                print(f'Base "{base}" not found')
            return {}

    cfg: Config = Config() if config is None else config
    con: dict[str, Base] | None = cfg.connections.get(instance)
    if con is None:
        if debug:
            print('No bases in instance')
        return {}
    s: str
    di: dict[str, Base]
    bases_dict: dict[str, Base] = {}
    if isinstance(bases, str):
        bases_dict = _gen_dict(con, bases, debug=debug)
    elif isinstance(bases, dict):
        v: str | Base
        for s, v in bases.items():
            if isinstance(v, str):
                di = _gen_dict(con, s, title=v, debug=debug)
                if di is not None:
                    bases_dict.update(di)
            else:
                bases_dict.update({base_names.get(s, s): v})
    elif isinstance(bases, Iterable):
        # bases_dict = {be: base_names.get(be, be) for be in bases}
        for s in bases:
            di = _gen_dict(con, s, debug=debug)
            if di is not None:
                bases_dict.update(di)
    else:
        if debug:
            print(f'Bases list of type "{type(bases)}" not supported')
        return {}
    # print(bases_dict)
    return bases_dict


def execute(sql: str,
            config: Config | None = None,
            instance: str = 'replica',
            bases: str | Iterable[str] | dict[str, str | Base]
            | None = None,
            insert_name=False,
            name_position=None,
            name_title='base',
            debug: bool = True,
            **kwargs) -> pd.DataFrame | None:
    bases_dict: dict[str, Base] = get_dict_of_bases(config=config,
                                                    instance=instance,
                                                    bases=bases,
                                                    debug=debug)
    b: Base
    df: pd.DataFrame | None
    ds: list[pd.DataFrame] = []
    for s, b in bases_dict.items():
        df = execute_on_base(sql, base=b, title=s, debug=debug)
        if df is not None:
            if len(df) != 0:
                if insert_name:
                    if name_position is not None:
                        df.insert(name_position, name_title, s)
                    else:
                        df.loc[:, name_title] = s
                ds.append(df)
    return pd.concat(ds, ignore_index=True) if len(ds) > 0 else None
