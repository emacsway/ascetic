from __future__ import absolute_import, unicode_literals
import re
from threading import local
from autumn.settings import DATABASES

connections = {}


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

    def describe_table(self, table_name):
        schema_name = None
        schema_sql = schema_name and "AND n.nspname = {0}".format(schema_name) or ""
        sql = """SELECT
                a.attnum,
                n.nspname,
                c.relname,
                a.attname AS colname,
                t.typname AS type,
                a.atttypmod,
                FORMAT_TYPE(a.atttypid, a.atttypmod) AS complete_type,
                d.adsrc AS default_value,
                a.attnotnull AS notnull,
                a.attlen AS length,
                co.contype,
                ARRAY_TO_STRING(co.conkey, ',') AS conkey
            FROM pg_attribute AS a
                JOIN pg_class AS c ON a.attrelid = c.oid
                JOIN pg_namespace AS n ON c.relnamespace = n.oid
                JOIN pg_type AS t ON a.atttypid = t.oid
                LEFT OUTER JOIN pg_constraint AS co ON (co.conrelid = c.oid
                    AND a.attnum = ANY(co.conkey) AND co.contype = 'p')
                LEFT OUTER JOIN pg_attrdef AS d ON d.adrelid = c.oid AND d.adnum = a.attnum
            WHERE a.attnum > 0 AND c.relname = {0}
            {1}
            ORDER BY a.attnum';
        """.format(table_name, schema_sql)
        cursor = self.cursor()
        cursor.execute(sql)
        fields = [f[0] for f in cursor.description]
        desc = {}
        for row in cursor.fetchall():
            data = dict(list(zip(fields, row)))
            default_value = data['default_value']
            if data['type'] in ('varchar', 'bpchar'):
                pass
            d = {
                'schema_name': data['nspname'],
                'table_name': data['relname'],
                'column_name': data['colname'],
                'column_position': data['attnum'],
                'data_type': data['type'],
                'default': default_value,
                'nullable': not data['notnull'],
                'length': data['length'],
                'scale': None,  # TODO
                'precision': None,  # TODO
                'unsigned': None,  # TODO
                'pimary': None,
                'primary_position': None,
                'identity': None,
            }
            desc[data[colname]] = d
        return desc

for name, conf in DATABASES.items():
    connections[name] = Database.factory(**conf)
