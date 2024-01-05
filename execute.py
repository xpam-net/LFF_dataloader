"""This is database connection module

This module for connecting to multiple databases and retrieving
data as a DataFrame
"""

__all__ = ['execute_on_base', 'get_dict_of_bases', 'execute']
__version__ = '0.2024.01.04.1'
__author__ = 'Andrey Luzhin'

import pandas as pd
import logging
#import urllib.parse
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sshtunnel import SSHTunnelForwarder
from dataloader.config import Config, Base
from dataloader.dicts import bases as base_names
from collections.abc import Iterable
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.cursor import CursorResult
from threading import Thread
from datetime import datetime

#
# Internal variables and functions
#

logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s',
                    force=True)
# '[%(asctime)s] %(module)s;%(name)s;%(funcName)s;%(levelname)s;%(message)s'
logger = logging.getLogger(__name__)

log_level: list[int] = [
    logging.NOTSET, logging.DEBUG, logging.INFO, logging.WARNING,
    logging.ERROR, logging.CRITICAL
]

def _loglevel(level: int | bool = 2) -> None:
    if isinstance(level, bool):
        logger.setLevel(logging.DEBUG if level else logging.WARNING)
    else:
        logger.setLevel(
            log_level[level] if 0 < level < len(log_level) else log_level[0])

#
# Public functions
#

def execute_on_base(sql: str,
                    base: Base | dict,
                    title: str | None = None,
                    debug: int | bool = 2,
                    **kwargs) -> pd.DataFrame | None:
    """Execute request on a single base

    Arguments:
        sql {str} -- SQL request
        base {Base | dict} -- base

    Keyword Arguments:
        title {str | None} -- title to show (default: {None})
        debug {bool} -- print debug information (default: {True})

    Returns:
        pd.DataFrame | None -- resulting DataFrame if data exists
    """

    _loglevel(debug)
    b = base if isinstance(base, Base) else Base().from_dict(base)
    logger.debug('%s: Opening connection...', title)
    start_time: datetime = datetime.now()
    tunnel: SSHTunnelForwarder | None = None
    if b.ssh is not None:
        tunnel = SSHTunnelForwarder(
            (b.ssh.host, b.ssh.port),
            ssh_username=b.ssh.login,
            ssh_pkey=b.ssh.key,
            ssh_password=b.ssh.password,
            allow_agent=False,  # disable id_rsa search
            remote_bind_address=(b.server, b.port))
    # if tunnel is not None:
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
    logger.info('%s: Connected', title)
    data: pd.DataFrame | None = None

    with engine.connect() as connection:
        #data: pd.DataFrame | None = pd.read_sql(sql, connection)
        #print(text(sql))
        result: CursorResult = connection.execute(text(sql))
        if result.returns_rows:
            # result.rowcount > 0: - is not suitable because
            # rowcount may not exist at all for SELECT statement.
            data = pd.DataFrame.from_records(result.fetchall(),
                                             columns=result.keys())
            #df = pd.DataFrame(result.fetchall())
            #df.columns = result.keys()
        else:
            logger.info('%s: Affected rows - %i', title, result.rowcount)
        connection.commit()  # It is possible to add an indentation to
        # apply only when there is no data,
        # but it's more secure that way.

    logger.info(f'{title}:'
                f' Done {"(empty)" if data is None else data.shape}'
                f' in {datetime.now() - start_time}')
    if tunnel is not None:
        tunnel.stop()
        # tunnel = None
    return data


def get_dict_of_bases(config: Config | None = None,
                      instance: str = 'replica',
                      bases: str | Iterable[str] | dict[str, str | Base]
                      | None = None,
                      debug: int | bool = 2) -> dict[str, Base]:
    _loglevel(debug)

    def _gen_dict(connections: dict[str, Base],
                  base: str,
                  title: str | None = None) -> dict[str, Base]:
        """Service function for proper dict generation

        Arguments:
            connections {dict[str, Base]} -- dict of connections
            base {str} -- base name

        Keyword Arguments:
            title {str | None} -- base title. If None, will search in
                global dict (default: {None})

        Returns:
            dict[str, Base] -- resulting dict
        """
        if connections.get(base) is not None:
            return {
                title if title is not None else base_names.get(base, base):
                connections[base]
            }
        else:
            logger.warning('Base %s not found', base)
            return {}

    cfg: Config = Config() if config is None else config
    con: dict[str, Base] | None = cfg.connections.get(instance)
    if con is None:
        logger.error('No bases in instance %s', instance)
        return {}
    s: str
    di: dict[str, Base]
    bases_dict: dict[str, Base] = {}
    if isinstance(bases, str):
        bases_dict = _gen_dict(con, bases)
    elif isinstance(bases, dict):
        v: str | Base
        for s, v in bases.items():
            if isinstance(v, str):
                di = _gen_dict(con, s, title=v)
                if di is not None:
                    bases_dict.update(di)
            else:
                bases_dict.update({base_names.get(s, s): v})
    elif isinstance(bases, Iterable):
        # bases_dict = {be: base_names.get(be, be) for be in bases}
        for s in bases:
            di = _gen_dict(con, s)
            if di is not None:
                bases_dict.update(di)
    else:
        logger.error('Bases list of type %s not supported', type(bases))
        return {}

    return bases_dict


def execute(sql: str,
            config: Config | None = None,
            instance: str = 'replica',
            bases: str | Iterable[str] | dict[str, str | Base]
            | None = None,
            insert_name: bool = False,
            name_position: int | None = None,
            name_title: str = 'base',
            no_threads: bool = False,
            debug: int | bool = 2,
            **kwargs) -> pd.DataFrame | None:
    """The main function of the module. Allows to execute SQL-query on
    several databases at once.
    Supports not only SELECT, but also other statements (INSERT,
    UPDATE, ALTER, etc.).

    Arguments:
        sql {str} -- SQL request

    Keyword Arguments:
        config {Config | None} -- _description_ (default: {None})
        instance {str} -- _description_ (default: {'replica'})
        bases {str | Iterable[str] | dict[str, str | Base] | None} -- single base or bases list (default: {None}) ???
        insert_name {bool} -- _description_ (default: {False})
        name_position {int | None} -- _description_ (default: {None})
        name_title {str} -- _description_ (default: {'base'})
        no_threads {bool} -- executing without threads (default: {False})
        debug {bool} -- print debug information (default: {True})

    Returns:
        None
    """

    bases_dict: dict[str, Base] = get_dict_of_bases(config=config,
                                                    instance=instance,
                                                    bases=bases,
                                                    debug=debug)
    b: Base
    df: pd.DataFrame | None
    ds: list[pd.DataFrame] = []
    threads: list[Thread] = []
    res: dict[str, pd.DataFrame | None] = {}

    def _execute(sql: str, res: dict[str, pd.DataFrame | None],
                 **kwargs) -> pd.DataFrame | None:
        """Service function for executing in threads

        Arguments:
            sql {str} -- SQL request
            res {dict[str, pd.DataFrame  |  None]} -- resulting dictionary

        Returns:
            pd.DataFrame | None -- resulting DataFrame if data exists
        """
        df: pd.DataFrame | None = execute_on_base(sql, **kwargs)
        if kwargs.get('title', None) is not None:
            res[kwargs['title']] = df
        return df

    for s, b in bases_dict.items():
        if not no_threads:
            # In general Execute from SQLAlchemy is not thread-safe,
            # but we are connecting to different databases,
            # so it doesn't matter.
            thread = Thread(target=_execute,
                            args=(
                                sql,
                                res,
                            ),
                            kwargs={
                                'base': b,
                                'title': s,
                                'debug': debug
                            } | kwargs)  # merging with **kwargs, python 3.9
            thread.start()
            threads.append(thread)
        else:
            # Just in case if we want to update neighboring bases,
            # we'll leave the option of not using threads.
            _execute(sql, res, base=b, title=s, debug=debug, **kwargs)

    if not no_threads:
        for thread in threads:
            thread.join()

    for s in bases_dict.keys():
        df = res.pop(s)
        if df is not None:
            if len(df) != 0:
                if insert_name:
                    if name_position is not None:
                        df.insert(name_position, name_title, [s] * df.shape[0])
                        # df.insert(name_position, name_title, pd.Series([s for _ in range(len(df.index))]))
                        # df.insert(name_position, name_title, s)
                        # The last one works and looks simpler,
                        # but causes an error with mypy.
                    else:
                        df.loc[:, name_title] = s
                ds.append(df)

    return pd.concat(ds, ignore_index=True) if len(ds) > 0 else None
