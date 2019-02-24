# !/usr/bin/python
# -*- coding:utf-8 -*-
# Author: Zhichang Fu
# Created Time: 2019-02-01 14:19:51
"""
async-crystaldb

"""

import asyncio
import aiomysql
from . import crystaldb
from .crystaldb.utils import storage
import functools
import warnings
import contextlib
import logging

try:
    asyncio_current_task = asyncio.current_task
except AttributeError:
    asyncio_current_task = asyncio.Task.current_task

__all__ = [
    "DatabaseInterface",
    "MySQLDatabase",
    "AsyncDatabase",
    "AsyncMySQLConnection",
]


class AsyncQueryWrapper:
    """Async query results wrapper for async `select()`. Internally uses
    results wrapper produced by sync peewee select query.

    Arguments:

        result_wrapper -- empty results wrapper produced by sync `execute()`
        call cursor -- async cursor just executed query

    To retrieve results after async fetching just iterate over this class
    instance, like you generally iterate over sync results wrapper.
    """

    def __init__(self, *, cursor=None, query=None):
        self._cursor = cursor
        self._rows = []
        self._result_cache = None
        #self._result_wrapper = self._get_result_wrapper(query)
        self._names = [x[0] for x in self._cursor.description]

    def __iter__(self):
        return iter(self._result_wrapper)

    def __len__(self):
        return len(self._rows)

    def __getitem__(self, idx):
        # NOTE: side effects will appear when both
        # iterating and accessing by index!
        if self._result_cache is None:
            self._result_cache = list(self)
        return self._result_cache[idx]

    #def get_result_wrapper(self, query):
    #    """Get result wrapper class.
    #    """
    #    cursor = RowsCursor(self._rows, self._cursor.description)
    #    return query._get_cursor_wrapper(cursor)

    async def fetchone(self):
        """Fetch single row from the cursor.
        """
        row = await self._cursor.fetchone()
        if not row:
            raise GeneratorExit
        self._rows.append(storage(dict(zip(self._names, row))))


############
# Database #
############


class AsyncMySQLConnection(object):
    """Asynchronous database connection pool.
    """

    def __init__(self, *, loop=None, timeout=None, **kwargs):
        self.pool = None
        self.loop = loop
        self.timeout = timeout
        self.connect_params = kwargs

    async def acquire(self):
        """Acquire connection from pool.
        """
        return (await self.pool.acquire())

    def release(self, conn):
        """Release connection to pool.
        """
        self.pool.release(conn)

    async def connect(self):
        """Create connection pool asynchronously.
        """
        self.pool = await aiomysql.create_pool(
            loop=self.loop,
            connect_timeout=self.timeout,
            **self.connect_params)

    async def close(self):
        """Terminate all pool connections.
        """
        self.pool.terminate()
        await self.pool.wait_closed()

    async def cursor(self, conn=None, *args, **kwargs):
        """Get cursor for connection from pool.
        """
        in_transaction = conn is not None
        if not conn:
            conn = await self.acquire()
        cursor = await conn.cursor(*args, **kwargs)
        cursor.release = functools.partial(
            self.release_cursor, cursor, in_transaction=in_transaction)
        return cursor

    async def release_cursor(self, cursor, in_transaction=False):
        """Release cursor coroutine. Unless in transaction,
        the connection is also released back to the pool.
        """
        conn = cursor.connection
        await cursor.close()
        if not in_transaction:
            self.release(conn)


class AsyncDatabase(object):
    _loop = None  # asyncio event loop
    _allow_sync = True  # whether sync queries are allowed
    _async_conn = None  # async connection
    _async_wait = None  # connection waiter
    _task_data = None  # asyncio per-task data

    def __setattr__(self, name, value):
        if name == 'allow_sync':
            warnings.warn(
                "`.allow_sync` setter is deprecated, use either the "
                "`.allow_sync()` context manager or `.set_allow_sync()` "
                "method.", DeprecationWarning)
            self._allow_sync = value
        else:
            super().__setattr__(name, value)

    @property
    def loop(self):
        """Get the event loop.

        If no event loop is provided explicitly on creating
        the instance, just return the current event loop.
        """
        return self._loop or asyncio.get_event_loop()

    async def connect_async(self, loop=None, timeout=None):
        """Set up async connection on specified event loop or
        on default event loop.
        """
        if self._async_conn:
            return
        elif self._async_wait:
            await self._async_wait
        else:
            self._loop = loop
            self._async_wait = asyncio.Future(loop=self._loop)

            conn = self._async_conn_cls(
                loop=self._loop, timeout=timeout, **self.connect_params_async)

            try:
                await conn.connect()
            except Exception as e:
                if not self._async_wait.done():
                    self._async_wait.set_exception(e)
                self._async_wait = None
                raise
            else:
                self._task_data = TaskLocals(loop=self._loop)
                self._async_conn = conn
                self._async_wait.set_result(True)

    async def cursor_async(self):
        """Acquire async cursor.
        """
        await self.connect_async(loop=self._loop)

        if self.transaction_depth_async() > 0:
            conn = self.transaction_conn_async()
        else:
            conn = None

        try:
            return (await self._async_conn.cursor(conn=conn))
        except Exception:
            await self.close_async()
            raise

    async def close_async(self):
        """Close async connection.
        """
        if self._async_wait:
            await self._async_wait
        if self._async_conn:
            conn = self._async_conn
            self._async_conn = None
            self._async_wait = None
            self._task_data = None
            await conn.close()

    async def push_transaction_async(self):
        """Increment async transaction depth.
        """
        await self.connect_async(loop=self.loop)
        depth = self.transaction_depth_async()
        if not depth:
            conn = await self._async_conn.acquire()
            self._task_data.set('conn', conn)
        self._task_data.set('depth', depth + 1)

    async def pop_transaction_async(self):
        """Decrement async transaction depth.
        """
        depth = self.transaction_depth_async()
        if depth > 0:
            depth -= 1
            self._task_data.set('depth', depth)
            if depth == 0:
                conn = self._task_data.get('conn')
                self._async_conn.release(conn)
        else:
            raise ValueError("Invalid async transaction depth value")

    def transaction_depth_async(self):
        """Get async transaction depth.
        """
        return self._task_data.get('depth', 0) if self._task_data else 0

    def transaction_conn_async(self):
        """Get async transaction connection.
        """
        return self._task_data.get('conn', None) if self._task_data else None

    #def transaction_async(self):
    #    """Similar to peewee `Database.transaction()` method, but returns
    #    asynchronous context manager.
    #    """
    #    return transaction(self)

    #def atomic_async(self):
    #    """Similar to peewee `Database.atomic()` method, but returns
    #    asynchronous context manager.
    #    """
    #    return atomic(self)

    #def savepoint_async(self, sid=None):
    #    """Similar to peewee `Database.savepoint()` method, but returns
    #    asynchronous context manager.
    #    """
    #    return savepoint(self, sid=sid)

    def set_allow_sync(self, value):
        """Allow or forbid sync queries for the database. See also
        the :meth:`.allow_sync()` context manager.
        """
        self._allow_sync = value

    @contextlib.contextmanager
    def allow_sync(self):
        """Allow sync queries within context. Close sync
        connection on exit if connected.

        Example::

            with database.allow_sync():
                PageBlock.create_table(True)
        """
        old_allow_sync = self._allow_sync
        self._allow_sync = True

        try:
            yield
        except:
            raise
        finally:
            try:
                self.close()
            except self.Error:
                pass  # already closed

        self._allow_sync = old_allow_sync

    def execute_sql(self, *args, **kwargs):
        """Sync execute SQL query, `allow_sync` must be set to True.
        """
        assert self._allow_sync, (
            "Error, sync query is not allowed! Call the `.set_allow_sync()` "
            "or use the `.allow_sync()` context manager.")
        if self._allow_sync in (logging.ERROR, logging.WARNING):
            logging.log(self._allow_sync,
                        "Error, sync query is not allowed: %s %s" %
                        (str(args), str(kwargs)))
        return super().execute_sql(*args, **kwargs)


class DatabaseInterface(object):

    _loop = None

    def __init__(self, database=None, **kwargs):
        assert database, ("Error, database must be provided via "
                          "argument or class member.")

        self.database = database

    @property
    def loop(self):
        """Get the event loop.

        If no event loop is provided explicitly on creating
        the instance, just return the current event loop.
        """
        if self._loop:
            return self._loop
        self._loop = asyncio.get_event_loop()
        return self._loop

    async def update(self, sql, values=None):
        cursor = await self.execute(sql, values)
        rowcount = cursor.rowcount
        await cursor.release()
        return rowcount

    async def insert(self, sql, values=None):
        cursor = await self.execute(sql, values)
        rowcount = cursor.rowcount
        await cursor.release()
        return rowcount

    async def delete(self, sql, values=None):
        cursor = await self.execute(sql, values)
        rowcount = cursor.rowcount
        await cursor.release()
        return rowcount

    async def query(self, sql, values=None):
        cursor = await self.execute(sql, values)
        result = AsyncQueryWrapper(cursor=cursor)
        try:
            while True:
                await result.fetchone()
        except GeneratorExit:
            pass
        finally:
            await cursor.release()
        return result._rows

    async def execute(self, sql, values=None):
        cursor = await self.database.cursor_async()

        try:
            query, params = self.database.raw_sql(sql, values)
            await cursor.execute(query, params)

        except Exception:
            await cursor.release()
            raise
        return cursor


class MySQLDatabase(AsyncDatabase, crystaldb.MySQLDB):
    """MySQL database driver providing **single drop-in sync** connection
    and **single async connection** interface.
    """
    if aiomysql:
        import pymysql
        Error = pymysql.Error

    def __init__(self, **kwargs):
        if not aiomysql:
            raise Exception("Error, aiomysql is not installed!")
        self.min_connections = 1
        self.max_connections = 1
        self._async_conn_cls = kwargs.pop('async_conn', AsyncMySQLConnection)
        super(MySQLDatabase, self).__init__(**kwargs)
        self.raw_sql_flag = True

    @property
    def connect_params_async(self):
        """Connection parameters for `aiomysql.Connection`
        """
        kwargs = self.params.copy()
        kwargs.update({
            'minsize': self.min_connections,
            'maxsize': self.max_connections,
            'autocommit': True,
        })
        return kwargs

    async def last_insert_id_async(self, cursor):
        """Get ID of last inserted row.
        """
        return cursor.lastrowid

    @property
    def use_speedups(self):
        return False

    @use_speedups.setter
    def use_speedups(self, value):
        pass


class TaskLocals:
    """Simple `dict` wrapper to get and set values on per `asyncio`
    task basis.

    The idea is similar to thread-local data, but actually *much* simpler.
    It's no more than a "sugar" class. Use `get()` and `set()` method like
    you would to for `dict` but values will be get and set in the context
    of currently running `asyncio` task.

    When task is done, all saved values are removed from stored data.
    """

    def __init__(self, loop):
        self.loop = loop
        self.data = {}

    def get(self, key, *val):
        """Get value stored for current running task. Optionally
        you may provide the default value. Raises `KeyError` when
        can't get the value and no default one is provided.
        """
        data = self.get_data()
        if data is not None:
            return data.get(key, *val)
        if val:
            return val[0]
        raise KeyError(key)

    def set(self, key, val):
        """Set value stored for current running task.
        """
        data = self.get_data(True)
        if data is not None:
            data[key] = val
        else:
            raise RuntimeError("No task is currently running")

    def get_data(self, create=False):
        """Get dict stored for current running task. Return `None`
        or an empty dict if no data was found depending on the
        `create` argument value.

        :param create: if argument is `True`, create empty dict
                       for task, default: `False`
        """
        task = asyncio_current_task(loop=self.loop)
        if task:
            task_id = id(task)
            if create and task_id not in self.data:
                self.data[task_id] = {}
                task.add_done_callback(self.del_data)
            return self.data.get(task_id)
        return None

    def del_data(self, task):
        """Delete data for task from stored data dict.
        """
        del self.data[id(task)]
