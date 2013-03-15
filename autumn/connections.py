from __future__ import absolute_import, unicode_literals
from threading import local
from autumn import settings

databases = {}


class Database(object):

    placeholder = '%s'

    def __init__(self, **kwargs):
        self.engine = kwargs.pop('engine')
        self.debug = kwargs.pop('debug', False)
        self.initial_sql = kwargs.pop('initial_sql', '')
        self._conf = kwargs
        self.ctx = local()
        self.ctx.b_commit = True

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

    def query(self, sql, params=()):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, params)
        except:
            cursor = self.reconnect().cursor()
            cursor.execute(sql, params)
        return cursor

    def cursor(self):
        try:
            return self.conn.cursor()
        except:
            return self.reconnect().cursor()

    def last_insert_id(self, cursor):
        return cursor.lastrowid

    @classmethod
    def factory(cls, **kwargs):
        relations = {
            'sqlite3': SqliteDatabase,
            'mysql': MySQLDatabase,
            'postgresql': PostgreSQLDatabase,
        }
        return relations.get(kwargs['engine'])(**kwargs)


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

for name, conf in settings.DATABASES.items():
    databases[name] = Database.factory(**conf)
