from __future__ import absolute_import, unicode_literals
import copy
from threading import local
from ..settings import DATABASES

connections = {}


class Database(object):
    placeholder = '?'
    
    def __init__(self):
        self.b_commit = True

    def connect(self, engine, *args, **kwargs):
        if engine == 'sqlite3':
            import sqlite3
            self.connection = sqlite3.connect(*args)
        elif engine == 'mysql':
            import MySQLdb
            self.connection = MySQLdb.connect(**kwargs)
            self.placeholder = '%s'


class DBConn(object):
    def __init__(self, conf=None):
        self.debug = conf.pop('debug', False)
        self._conf = conf
        self.ctx = local()

    @property
    def conn(self):
        try:
            return self.ctx.conn
        except AttributeError:
            return self.reconnect()

    def reconnect(self):
        self.ctx.conn = Database()
        self.ctx.conn.connect(**self._conf)
        return self.ctx.conn

    def query(self, sql, params=()):
        try:
            cursor = self.conn.connection.cursor()
            cursor.execute(sql, params)
        except:
            cursor = self.reconnect().connection.cursor()
            cursor.execute(sql, params)
        return cursor

    def cursor(self):
        try:
            return self.conn.connection.cursor()
        except:
            return self.reconnect().connection.cursor()
        return cursor

for name, conf in DATABASES.items():
    connections[name] = DBConn(conf=conf)
