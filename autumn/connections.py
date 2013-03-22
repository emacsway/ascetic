from __future__ import absolute_import, unicode_literals
from threading import local
from autumn import settings

PLACEHOLDER = '%s'

databases = {}


class Database(object):

    placeholder = '%s'

    def __init__(self, **kwargs):
        self.engine = kwargs.pop('engine')
        self.debug = kwargs.pop('debug', False)
        self.initial_sql = kwargs.pop('initial_sql', '')
        self._conf = kwargs
        self.ctx = local()
        self.ctx.autocommit = True
        self.ctx.begin_level = 0

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
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, params)
        except:
            cursor = self.reconnect().cursor()
            cursor.execute(sql, params)
        return cursor

    def execute(self, sql, params=(), using=None):
        if self.debug:
            print(sql, params)
        if self.placeholder != PLACEHOLDER:
            sql = sql.replace(PLACEHOLDER, self.placeholder)
        try:
            cursor = self._execute(sql, params)
            if self.get_autocommit() and self.ctx.begin_level == 0:
                self.commit()
        except BaseException as ex:
            if self.debug:
                print("_execute: exception: ", ex)
                print("sql:", sql)
                print("params:", params)
                raise
        else:
            return cursor

    def cursor(self):
        try:
            return self.conn.cursor()
        except:
            return self.reconnect().cursor()

    def last_insert_id(self, cursor):
        return cursor.lastrowid

    def get_autocommit(self):
        return self.ctx.autocommit

    def set_autocommit(self, autocommit=True):
        self.ctx.autocommit = autocommit
        return self

    def begin(self):
        self.ctx.begin_level += 1

    def commit(self):
        try:
            self.conn.commit()
        finally:
            if self.ctx.begin_level > 0:
                self.ctx.begin_level -= 1

    def rollback(self):
        try:
            self.conn.rollback()
        finally:
            if self.ctx.begin_level > 0:
                self.ctx.begin_level -= 1

    @classmethod
    def factory(cls, **kwargs):
        relations = {
            'sqlite3': SqliteDatabase,
            'mysql': MySQLDatabase,
            'postgresql': PostgreSQLDatabase,
            'django': DjangoDatabase,
        }
        return relations.get(kwargs['engine'])(**kwargs)


class DjangoDatabase(Database):

    DJANGO_ENGINES = {
        'sqlite3': 'sqlite3',
        'mysql': 'mysql',
        'postgresql': 'postgres',
        'postgresql_psycopg2': 'postgres',
        'postgis': 'postgres',
        'oracle': 'oracle',
    }

    def __init__(self, **kwargs):
        self.django_using = kwargs.pop('django_using')
        self.debug = kwargs.pop('debug', False)
        self.initial_sql = kwargs.pop('initial_sql', '')
        self._conf = kwargs
        self.ctx = local()
        self.ctx.autocommit = True
        self.ctx.begin_level = 0

    @property
    def django_conn(self):
        from django.db import connections
        return connections[self.django_using]

    @property
    def engine(self):
        return self.DJANGO_ENGINES.get(
            self.django_conn.settings_dict['ENGINE'].rsplit('.')[-1]
        )

    @property
    def conn(self):
        self.django_conn.ensure_connection()
        return self.django_conn.connection

    def cursor(self):
        return self.django_conn.cursor()

    def get_autocommit(self):
        from django.db.transaction import get_autocommit
        return get_autocommit(self.django_using)

    def set_autocommit(self, autocommit=True):
        from django.db.transaction import set_autocommit
        return set_autocommit(autocommit, self.django_using)


class SqliteDatabase(Database):

    placeholder = '?'

    def _connect(self, *args, **kwargs):
        import sqlite3
        return sqlite3.connect(*args)


class MySQLDatabase(Database):

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


def get_db(using=None):
    return databases[using or 'default']


for name, conf in settings.DATABASES.items():
    databases[name] = Database.factory(**conf)
