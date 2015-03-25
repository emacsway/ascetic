from __future__ import absolute_import, unicode_literals
import collections
import logging
from functools import wraps
from threading import local
from sqlbuilder import smartsql
from sqlbuilder.smartsql.compilers import mysql, sqlite
from autumn import settings

PLACEHOLDER = '%s'

# TODO: thread safe dict emulating?
databases = {}


class DummyCtx(object):
    pass


class Transaction(object):

    def __init__(self, using='default'):
        """Constructor of Transaction instance."""
        self.db = get_db(using)

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

    def __init__(self, **kwargs):
        self.using = kwargs.pop('using')
        self.logger = logging.getLogger('.'.join((__name__, self.using)))
        self.engine = kwargs.pop('engine')
        self.debug = kwargs.pop('debug', False)
        self.initial_sql = kwargs.pop('initial_sql', '')
        self.thread_safe = kwargs.pop('thread_safe', True)
        self._conf = kwargs
        self.ctx = local() if self.thread_safe else DummyCtx()

    def _connect(self, *args, **kwargs):
        raise NotImplementedError

    @property
    def conn(self):
        try:
            return self.ctx.conn
        except AttributeError:
            return self.reconnect()

    def reconnect(self):
        self.ctx.conn = self._connect(**self._conf)
        if self.initial_sql:
            self.conn.cursor().execute(self.initial_sql)
        return self.ctx.conn

    def _execute(self, sql, params=()):
        cursor = self.cursor()
        try:
            cursor.execute(sql, params)
        except Exception:
            cursor = self.reconnect().cursor()
            cursor.execute(sql, params)
        return cursor

    def execute(self, sql, params=()):
        if isinstance(sql, smartsql.QuerySet):
            sql, params = self.compile(sql)
        if self.debug:
            self.logger.debug("%s - %s", sql, params)
        if self.placeholder != PLACEHOLDER:
            sql = sql.replace(PLACEHOLDER, self.placeholder)
        try:
            cursor = self._execute(sql, params)
            if self.get_autocommit() and self.begin_level() == 0:
                self.commit()
        except BaseException as e:
            if self.debug:
                self.logger.exception(e)
            raise
        else:
            return cursor

    def cursor(self):
        try:
            return self.conn.cursor()
        except Exception:
            return self.reconnect().cursor()

    def last_insert_id(self, cursor):
        return cursor.lastrowid

    def get_autocommit(self):
        return getattr(self.ctx, 'autocommit', True)

    def set_autocommit(self, autocommit=True):
        self.ctx.autocommit = autocommit
        return self

    def begin_level(self, val=None):
        if not hasattr(self.ctx, 'begin_level'):
            self.ctx.begin_level = 0
        if val is not None:
            self.ctx.begin_level += val
        if self.ctx.begin_level < 0:
            self.ctx.begin_level = 0
        return self.ctx.begin_level

    @property
    def transaction(self):
        return Transaction(self.using)

    def begin(self):
        self.begin_level(+1)

    def commit(self):
        try:
            self.conn.commit()
        finally:
            self.begin_level(-1)

    def rollback(self):
        try:
            self.conn.rollback()
        finally:
            self.begin_level(-1)

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

    @property
    def django_conn(self):
        from django.db import connections
        return connections[self.django_using]

    @property
    def conn(self):
        # self.django_conn.ensure_connection()
        if not self.django_conn.connection:
            self.cursor()
        return self.django_conn.connection

    def cursor(self):
        return self.django_conn.cursor()

    def get_autocommit_(self):
        from django.db.transaction import get_autocommit
        return get_autocommit(self.django_using)

    def set_autocommit_(self, autocommit=True):
        from django.db.transaction import set_autocommit
        return set_autocommit(autocommit, self.django_using)


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


class PostgreSQLDatabase(Database):

    def _connect(self, *args, **kwargs):
        import psycopg2
        return psycopg2.connect(**kwargs)

    def last_insert_id(self, cursor):
        cursor.execute("SELECT lastval()")
        return cursor.fetchone()[0]

    def describe_table(self, table_name):
        cursor = self.execute("""
            SELECT * FROM information_schema.columns WHERE table_name = %s;
        """, [table_name])
        fields = [f[0] for f in cursor.description]
        schema = collections.OrderedDict()
        for row in cursor.fetchall():
            data = dict(list(zip(fields, row)))
            col = {
                'column': data['column_name'],
                'position': data['ordinal_position'],
                'type': data['udt_name'],
                'data_type': data['data_type'],
                'null': data['is_nullable'].upper() == 'YES',
                'max_length': data['character_maximum_length'],
            }
            schema[col['column']] = col
        return schema


def get_db(using=None):
    return databases[using or 'default']


for name, conf in settings.DATABASES.items():
    databases[name] = Database.factory(using=name, **conf)
