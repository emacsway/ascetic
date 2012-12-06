from __future__ import absolute_import, unicode_literals
import copy
from threading import local
from autumn.settings import DATABASES

connections = {}


class Database(object):

    placeholder = '%s'

    def __init__(self, **kwargs):
        self.engine = kwargs.pop('engine')
        self.debug = kwargs.pop('debug', False)
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
        return cursor

    @classmethod
    def factory(cls, **kwargs):
        relations = {
            'sqlite3': SqliteDatabase,
            'mysql': MySQLDatabase,
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

for name, conf in DATABASES.items():
    connections[name] = Database.factory(**conf)
