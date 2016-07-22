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
from ascetic import interfaces
from ascetic import utils

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

utils.resolve(settings.LOGGER_INIT)(settings)


class BaseTransaction(interfaces.ITransaction):
    def __init__(self, using):
        self._using = using

    def parent(self):
        return None

    def can_reconnect(self):
        return False

    def set_autocommit(self, autocommit):
        raise Exception("You cannot set autocommit during a managed transaction!")

    @utils.cached_property
    def _db(self):
        return databases[self._using]


class Transaction(BaseTransaction):

    def begin(self):
        self._db.execute("BEGIN")

    def commit(self):
        self._db.commit()
        self._clear_identity_map()

    def rollback(self):
        self._db.rollback()
        self._clear_identity_map()

    def _clear_identity_map(self):
        from ascetic.models import IdentityMap
        IdentityMap(self._using).clear()


class SavePoint(BaseTransaction):
    def __init__(self, using, parent, name=None):
        BaseTransaction.__init__(self, using)
        self._parent = parent
        self._name = name or 's' + uuid4().hex

    def parent(self):
        return self._parent

    def begin(self, name=None):
        self._db.begin_savepoint(self._name)

    def commit(self):
        self._db.commit_savepoint(self._name)

    def rollback(self):
        self.rollback_savepoint(self._name)


class NoneTransaction(BaseTransaction):
    def begin(self, name=None):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def can_reconnect(self):
        return True

    def set_autocommit(self, autocommit):
        self._db.set_autocommit(autocommit)


class TransactionManager(interfaces.ITransactionManager):
    def __init__(self, using, autocommit):
        self._using = using
        self._current = None
        self._autocommit = autocommit

    def __call__(self, func=None):
        if func is None:
            return self

        @wraps(func)
        def _decorated(*a, **kw):
            with self:
                rv = func(*a, **kw)
            return rv

        return _decorated

    def __enter__(self):
        self.begin()

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                self.rollback()
            else:
                try:
                    self.commit()
                except:
                    self.rollback()
                    raise
        finally:
            pass

    def current(self, node=utils.Undef):
        if node is utils.Undef:
            return self._current or NoneTransaction(self._using)
        self._current = node

    def begin(self):
        if self._current is None:
            self.current().set_autocommit(False)
            self.current(Transaction(self._using))
        else:
            self.current(SavePoint(self._using, self.current()))
        self.current().begin()
        return

    def commit(self):
        self.current().commit()
        self.current(self.current().parent())

    def rollback(self):
        self.current().rollback()
        self.current(self.current().parent())

    def can_reconnect(self):
        return self.current().can_reconnect()

    def on_connect(self):
        self._current = None
        self.current().set_autocommit(self._autocommit)

    def autocommit(self, autocommit=None):
        if autocommit is None:
            return self._autocommit and not self._current
        self._autocommit = autocommit
        self.current().set_autocommit(autocommit)


class Database(object):

    _engines = {}
    placeholder = '%s'
    compile = smartsql.compile
    connection = None

    def __init__(self, alias, engine, transaction, initial_sql, always_reconnect=False, debug=False, **kwargs):
        self.alias = alias
        self.engine = engine
        self.debug = debug
        self.initial_sql = initial_sql
        self.always_reconnect = always_reconnect
        self.transaction = transaction
        self._conf = kwargs
        self._logger = logging.getLogger('.'.join((__name__, self.alias)))

        if self.debug:
            self._execute = self.log_sql(self._execute)

    def connection_factory(self, **kwargs):
        raise NotImplementedError

    def _ensure_connected(self):
        self.connection = self.connection_factory(**self._conf)
        self.transaction.on_connect()
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
            if self.always_reconnect or self.transaction.can_reconnect():
                self._ensure_connected()
                cursor = self.cursor()
                cursor.execute(sql, params)
            else:
                raise
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

    def begin(self):
        self.execute("BEGIN")

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def begin_savepoint(self, name):
        self.execute("SAVEPOINT %s", name)

    def commit_savepoint(self, name):
        self.execute("RELEASE SAVEPOINT %s", name)

    def rollback_savepoint(self, name):
        self.execute("ROLLBACK TO SAVEPOINT %s", name)

    def set_autocommit(self, autocommit):
        pass

    def read_fields(self, db_table):
        schema = self.describe_table(db_table)
        q = self.execute('SELECT * FROM {0} LIMIT 1'.format(self.qn(db_table)))
        # See cursor.description http://www.python.org/dev/peps/pep-0249/
        result = []
        for row in q.description:
            column = row[0]
            data = schema.get(column) or {}
            data.update({'column': column, 'type_code': row[1]})
            result.append(data)
        return result

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
            return engine_cls
        return _deco

    @classmethod
    def factory(cls, **kwargs):
        transaction = TransactionManager(kwargs['alias'], kwargs.pop('autocommit', False))
        try:
            Cls = cls._engines[kwargs['engine']]
        except KeyError:
            Cls = utils.resolve(kwargs['engine'])

        database = Cls(transaction=transaction, **kwargs)
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

    def read_pk(self, table_name):
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

    def set_autocommit(self, autocommit):
        self.execute("SET autocommit={}".format(int(autocommit)))
        # self.connection.autocommit(autocommit)
        super(MySQLDatabase, self).set_autocommit(autocommit)


@Database.register('postgresql')
class PostgreSQLDatabase(Database):

    def connection_factory(self, **kwargs):
        import psycopg2
        return psycopg2.connect(**kwargs)

    def last_insert_id(self, cursor):
        cursor.execute("SELECT lastval()")
        return cursor.fetchone()[0]

    def read_pk(self, table_name):
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

    def set_autocommit(self, autocommit):
        # The server-side autocommit setting was removed and reimplemented in client applications and languages.
        # Server-side autocommit was causing too many problems with languages and applications that wanted
        # to control their own autocommit behavior, so autocommit was removed from the server and added to individual
        # client APIs as appropriate.
        # https://www.postgresql.org/docs/7.4/static/release-7-4.html
        # self.execute("SET AUTOCOMMIT = {}".format('ON' if autocommit else 'OFF'))
        self.connection.set_session(autocommit=autocommit)
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
