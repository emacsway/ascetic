from __future__ import absolute_import
import collections
import logging
from functools import wraps
from threading import local
from uuid import uuid4
from sqlbuilder import smartsql
from sqlbuilder.smartsql.compilers import mysql, sqlite
from autumn import settings
from autumn.utils import resolve

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

    placeholder = '%s'
    compile = smartsql.compile
    connection = None
    _begin_level = 0
    _autocommit = False

    def __init__(self, **kwargs):
        self.using = kwargs.pop('using')
        self.logger = logging.getLogger('.'.join((__name__, self.using)))
        self.engine = kwargs.pop('engine')
        self.debug = kwargs.pop('debug', False)
        self.initial_sql = kwargs.pop('initial_sql', '')
        self.always_reconnect = kwargs.pop('always_reconnect', False)
        self.autocommit = kwargs.pop('autocommit', False)
        self._savepoints = []
        self._conf = kwargs

    def _connect(self, *args, **kwargs):
        raise NotImplementedError

    def _ensure_connected(self):
        self.connection = self._connect(**self._conf)
        if self.autocommit:
            self.set_autocommit(self.autocommit)
        if self.initial_sql:
            self.connection.cursor().execute(self.initial_sql)
        return self

    def _execute(self, sql, params=()):
        cursor = self.cursor()
        try:
            cursor.execute(sql, params)
        except Exception:
            if self._begin_level > 0 and not self.always_reconnect:
                raise
            self._ensure_connected()
            cursor = self.cursor()
            cursor.execute(sql, params)
        return cursor

    def execute(self, sql, params=()):
        if not isinstance(sql, string_types):
            sql, params = self.compile(sql)
        if self.debug:
            self.logger.debug("%s - %s", sql, params)
        if self.placeholder != PLACEHOLDER:
            sql = sql.replace(PLACEHOLDER, self.placeholder)
        try:
            cursor = self._execute(sql, params)
        except BaseException as e:
            if self.debug:
                self.logger.exception(e)
            raise
        else:
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
        return Transaction(self.using)

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
        else:
            self.savepoint_commit()

    def rollback(self):
        self._begin_level = max(0, self._begin_level - 1)
        if self._begin_level == 0:
            self.connection.rollback()
        else:
            self.savepoint_rollback()

    def savepoint_begin(self):
        self._savepoints.append('s' + uuid4().hex)
        self.execute("SAVEPOINT %s", self._last_savepoint)

    def savepoint_commit(self):
        self.execute("RELEASE SAVEPOINT %s", self._savepoints.pop())

    def savepoint_rollback(self):
        self.execute("ROLLBACK TO SAVEPOINT %s", self._savepoints.pop())

    def describe_table(self, table_name):
        return {}

    def qn(self, name):
        return self.compile(smartsql.Name(name))[0]

    @classmethod
    def factory(cls, **kwargs):
        relations = {
            'sqlite3': SqliteDatabase,
            'mysql': MySQLDatabase,
            'postgresql': PostgreSQLDatabase,
        }
        Cls = relations.get(kwargs['engine'])
        if 'django_using' in kwargs:
            class Cls(DjangoMixin, Cls):
                pass
        return Cls(**kwargs)


class DjangoMixin(object):

    def __init__(self, **kwargs):
        self.django_using = kwargs.pop('django_using')
        super(DjangoMixin, self).__init__(**kwargs)

    def get_django_connection(self):
        from django.db import connections
        return connections[self.django_using]

    def _connect(self, *args, **kwargs):
        # self.django_conn.ensure_connection()
        django_connection = self.get_django_connection()
        if not django_connection.connection:
            django_connection.cursor()
        return django_connection.connection


class SqliteDatabase(Database):

    placeholder = '?'
    compile = sqlite.compile

    def _connect(self, *args, **kwargs):
        import sqlite3
        return sqlite3.connect(*args)


class MySQLDatabase(Database):

    compile = mysql.compile

    def _connect(self, *args, **kwargs):
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


class PostgreSQLDatabase(Database):

    def _connect(self, *args, **kwargs):
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

    def __getitem__(self, alias):
        try:
            return getattr(self._databases, alias)
        except AttributeError:
            setattr(self._databases, alias, Database.factory(using=alias, **self._settings[alias]))
            return getattr(self._databases, alias)


def get_db(using=None):
    smartsql.warn('get_db', 'databases')
    return databases[using or 'default']


databases = Databases(settings.DATABASES)
