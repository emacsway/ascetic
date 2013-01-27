from __future__ import absolute_import, unicode_literals
from .connection import connections
from sqlbuilder.smartsql import PLACEHOLDER

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)


class Query(object):

    @classmethod
    def get_db(cls, using=None):
        if not using:
            using = getattr(cls, 'using', 'default')
        return connections[using]

    @classmethod
    def get_cursor(cls, using=None):
        return cls.get_db(using).cursor()

    @classmethod
    def raw_sql(cls, sql, params=(), using=None):
        db = cls.get_db(using)
        if db.debug:
            print(sql, params)
        cursor = cls.get_cursor(using)
        if db.placeholder != PLACEHOLDER:
            sql = sql.replace(PLACEHOLDER, db.placeholder)
        try:
            cursor.execute(sql, params)
            if db.ctx.b_commit:
                db.conn.commit()
        except BaseException as ex:
            if db.debug:
                print("raw_sql: exception: ", ex)
                print("sql:", sql)
                print("params:", params)
            raise
        return cursor

    @classmethod
    def raw_sqlscript(cls, sql, using=None):
        db = cls.get_db(using)
        cursor = cls.get_cursor(using)
        try:
            cursor.executescript(sql)
            if db.ctx.b_commit:
                db.conn.commit()
        except BaseException as ex:
            if db.debug:
                print("raw_sqlscript: exception: ", ex)
                print("sql:", sql)
            raise
        return cursor

    # begin() and commit() for SQL transaction control
    # This has only been tested with SQLite3 with default isolation level.
    # http://www.python.org/doc/2.5/lib/sqlite3-Controlling-Transactions.html
    @classmethod
    def begin(cls, using=None):
        """
        begin() and commit() let you explicitly specify an SQL transaction.
        Be sure to call commit() after you call begin().
        """
        cls.get_db(using).ctx.b_commit = False

    @classmethod
    def commit(cls, using=None):
        """
        begin() and commit() let you explicitly specify an SQL transaction.
        Be sure to call commit() after you call begin().
        """
        cursor = None
        try:
            cls.get_db(using).conn.commit()
        finally:
            cls.get_db(using).ctx.b_commit = True
        return cursor
