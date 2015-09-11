from __future__ import absolute_import
import os
import logging
import collections
from time import time
from functools import wraps
from threading import local
from uuid import uuid4
from sqlbuilder import smartsql
from sqlbuilder.smartsql.compilers import mysql, sqlite
from ascetic import settings
from ascetic.utils import resolve

try:
    import _thread
except ImportError:
    import thread as _thread  # Python < 3.*

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)

PLACEHOLDER = '%s'

resolve(settings.LOGGER_INIT)(settings)


class Transaction(object):

    def __init__(self, using='default'):
        """Constructor of Transaction instance."""
        self.db = databases[using]

    def __call__(self, f=None):
        if f is None:
            return self

        @wraps(f)
        def _decorated(*args, **kw):
            with self:
                rv = f(*args, **kw)
            return rv

        return _decorated

    def __enter__(self):
        self.db.begin()

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                self.db.rollback()
            else:
                try:
                    self.db.commit()
                except:
                    self.db.rollback()
                    raise
        finally:
            pass


class Database(object):

    _engines = {}
    placeholder = '%s'
    compile = smartsql.compile
    connection = None
    _begin_level = 0
    _autocommit = False

    def __init__(self, alias, engine, initial_sql, always_reconnect=False, autocommit=False, debug=False, **kwargs):
        self.alias = alias
        self.engine = engine
        self.debug = debug
        self.initial_sql = initial_sql
        self.always_reconnect = always_reconnect
        self.autocommit = autocommit
        self._savepoints = []
        self._conf = kwargs
        self._logger = logging.getLogger('.'.join((__name__, self.alias)))

        if self.debug:
            self._execute = self.log_sql(self._execute)

    def connection_factory(self, **kwargs):
        raise NotImplementedError

    def _ensure_connected(self):
        self.connection = self.connection_factory(**self._conf)
        if self.autocommit:
            self.set_autocommit(self.autocommit)
        if self.initial_sql:
            self.connection.cursor().execute(self.initial_sql)
        return self

    def log_sql(self, f):
        alias = self.alias
        logger = self._logger

        @wraps(f)
        def wrapper(sql, params=()):
            start = time()
            try:
                return f(sql, params)
            except Exception as e:
                logger.exception(e)
                raise
            finally:
                stop = time()
                duration = stop - start
                logger.debug(
                    '%s - (%.4f) %s; args=%s' % (alias, duration, sql, params),
                    extra={'alias': alias, 'duration': duration, 'sql': sql, 'params': params}
                )
        return wrapper

    def _execute(self, sql, params=()):
        cursor = self.cursor()
        try:
            cursor.execute(sql, params)
        except Exception:
            if (not self._autocommit or self._begin_level > 0) and not self.always_reconnect:
                raise
            self._ensure_connected()
            cursor = self.cursor()
            cursor.execute(sql, params)
        return cursor

    def execute(self, sql, params=()):
        if not isinstance(sql, string_types):
            sql, params = self.compile(sql)
        if self.placeholder != PLACEHOLDER:
            sql = sql.replace(PLACEHOLDER, self.placeholder)
        sql = sql.rstrip("; \t\n\r")
        cursor = self._execute(sql, params)
        return cursor

    def cursor(self):
        if not self.connection:
            self._ensure_connected()
        return self.connection.cursor()

    def last_insert_id(self, cursor):
        return cursor.lastrowid

    def get_autocommit(self):
        return self._autocommit and self._begin_level == 0

    def set_autocommit(self, autocommit=True):
        self._autocommit = autocommit
        return self

    @property
    def transaction(self):
        return Transaction(self.alias)

    def begin(self):
        if self._begin_level == 0:
            self.execute("BEGIN")
        else:
            self.savepoint_begin()
        self._begin_level += 1

    def commit(self):
        self._begin_level = max(0, self._begin_level - 1)
        if self._begin_level == 0:
            self.connection.commit()
            from ascetic.models import IdentityMap
            IdentityMap().clear()
        else:
            self.savepoint_commit()

    def rollback(self):
        self._begin_level = max(0, self._begin_level - 1)
        if self._begin_level == 0:
            self.connection.rollback()
            from ascetic.models import IdentityMap
            IdentityMap().clear()
        else:
            self.savepoint_rollback()

    def savepoint_begin(self, name=None):
        if name is None:
            name = 's' + uuid4().hex
        self._savepoints.append(name)
        self.execute("SAVEPOINT %s", name)

    def savepoint_commit(self, name=None):
        if name is None:
            name = self._savepoints.pop()
        else:
            del self._savepoints[self._savepoints.index(name):]
        self.execute("RELEASE SAVEPOINT %s", name)

    def savepoint_rollback(self, name=None):
        if name is None:
            name = self._savepoints.pop()
        else:
            del self._savepoints[self._savepoints.index(name):]
        self.execute("ROLLBACK TO SAVEPOINT %s", name)

    def describe_table(self, table_name):
        return {}

    def qn(self, name):
        return self.compile(smartsql.Name(name))[0]

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    @classmethod
    def register(cls, engine):
        def _deco(engine_cls):
            cls._engines[engine] = engine_cls
            return cls
        return _deco

    @classmethod
    def factory(cls, **kwargs):
        try:
            Cls = cls._engines[kwargs['engine']]
        except KeyError:
            Cls = resolve(kwargs['engine'])
        database = Cls(**kwargs)
        if 'django_alias' in kwargs:
            database.connection_factory = django_connection_factory
        return database


def django_connection_factory(django_alias, **kwargs):
    # self.django_conn.ensure_connection()
    from django.db import connections
    django_connection = connections[django_alias]
    if not django_connection.connection:
        django_connection.cursor()
    return django_connection.connection


@Database.register('sqlite3')
class SqliteDatabase(Database):

    placeholder = '?'
    compile = sqlite.compile

    def connection_factory(self, **kwargs):
        import sqlite3
        return sqlite3.connect(**kwargs)


@Database.register('mysql')
class MySQLDatabase(Database):

    compile = mysql.compile

    def connection_factory(self, **kwargs):
        import MySQLdb
        return MySQLdb.connect(**kwargs)

    def get_pk(self, table_name):
        cursor = self.execute("""
            SELECT COLUMN_NAME
            FROM   information_schema.KEY_COLUMN_USAGE
            WHERE  TABLE_SCHEMA = SCHEMA()
            AND    CONSTRAINT_NAME = 'PRIMARY'
            AND    TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION;
        """, [table_name])
        return tuple(i[0] for i in cursor.fetchall())

    def describe_table(self, table_name):
        cursor = self.execute("""
            SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH
            FROM   information_schema.COLUMNS
            WHERE  TABLE_SCHEMA = SCHEMA()
            AND    TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION;
        """, [table_name])
        schema = collections.OrderedDict()
        for row in cursor.fetchall():
            col = {
                'column': row[0],
                'position': row[1],
                'data_type': row[2],
                'null': row[3].upper() == 'YES',
                # 'default': row[4],
                'max_length': row[5],
            }
            schema[col['column']] = col
        return schema

    def set_autocommit(self, autocommit=True):
        self.execute("SET autocommit={}".format(int(autocommit)))
        super(MySQLDatabase, self).set_autocommit(autocommit)


@Database.register('postgresql')
class PostgreSQLDatabase(Database):

    def connection_factory(self, **kwargs):
        import psycopg2
        return psycopg2.connect(**kwargs)

    def last_insert_id(self, cursor):
        cursor.execute("SELECT lastval()")
        return cursor.fetchone()[0]

    def get_pk(self, table_name):
        # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
        cursor = self.execute("""
        SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
        FROM   pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid
               AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = %s::regclass
               AND i.indisprimary
        ORDER BY a.attnum;
        """, [table_name])
        return tuple(i[0] for i in cursor.fetchall())

    def describe_table(self, table_name):
        cursor = self.execute("""
            SELECT column_name, ordinal_position, data_type, is_nullable, column_default, character_maximum_length
            FROM   information_schema.columns
            WHERE  table_name = %s
            ORDER BY ordinal_position;
        """, [table_name])
        schema = collections.OrderedDict()
        for row in cursor.fetchall():
            col = {
                'column': row[0],
                'position': row[1],
                'data_type': row[2],
                'null': row[3].upper() == 'YES',
                # 'default': row[4],
                'max_length': row[5],
            }
            schema[col['column']] = col
        return schema

    def set_autocommit(self, autocommit=True):
        self.execute("SET AUTOCOMMIT = {}".format('ON' if autocommit else 'OFF'))
        super(PostgreSQLDatabase, self).set_autocommit(autocommit)


class Databases(object):

    def __init__(self, conf):
        self._settings = conf
        self._databases = local()

    def create_database(self, alias):
        return Database.factory(alias=alias, **self._settings[alias])

    @staticmethod
    def get_thread_id():
        """Returs id for current thread."""
        return (os.getpid(), _thread.get_ident())

    def close(self):
        for alias in self:
            del self[alias]

    def __getitem__(self, alias):
        try:
            # Prevent situation like this:
            # http://stackoverflow.com/a/7285933
            # http://stackoverflow.com/questions/7285541/pythons-multiprocessing-does-not-play-nicely-with-threading-local
            # A fork() completely duplicates the process object, along with its
            # memory, loaded code, open file descriptors and threads.
            # Moreover, the new process usually shares the very same process
            # object within the kernel until the first memory write operation.
            # This basically means that the local data structures are also being
            # copied into the new process, along with the thread local variables.
            # return getattr(self._databases, alias)
            db = getattr(self._databases, alias)
            if db._thread_id != _thread.get_ident():
                raise ValueError
            return db
        except (AttributeError, ValueError):
            db = self.create_database(alias)
            db._thread_id = _thread.get_ident()
            setattr(self._databases, alias, db)
            return db

    def __delitem__(self, alias):
        if hasattr(self._databases, alias):
            getattr(self._databases, alias).close()
            delattr(self._databases, alias)

    def __iter__(self):
        return iter(self._settings)


def get_db(alias=None):
    smartsql.warn('get_db', 'databases')
    return databases[alias or 'default']


databases = Databases(settings.DATABASES)
