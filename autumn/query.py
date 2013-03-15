from __future__ import absolute_import, unicode_literals
from .connections import databases
from sqlbuilder.smartsql import PLACEHOLDER

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)


def get_db(using=None):
    using = using or 'default'
    return databases[using]


def get_cursor(using=None):
    return get_db(using).cursor()


def _execute(attr, sql, params=(), using=None):
    db = get_db(using)
    if db.debug:
        print(sql, params)
    cursor = get_cursor(using)
    if db.placeholder != PLACEHOLDER:
        sql = sql.replace(PLACEHOLDER, db.placeholder)
    try:
        getattr(cursor, attr)(sql, params)
        if db.ctx.b_commit:
            db.conn.commit()
    except BaseException as ex:
        if db.debug:
            print("_execute: exception: ", ex)
            print("sql:", sql)
            print("params:", params)
            raise
    return cursor


def execute(sql, params=(), using=None):
    return _execute('execute', sql, params, using)


def executemany(sql, params=(), using=None):
    return _execute('executemany', sql, params, using)


def executescript(sql, params=(), using=None):
    return _execute('executescript', sql, params, using)


# begin() and commit() for SQL transaction control
# This has only been tested with SQLite3 with default isolation level.
# http://www.python.org/doc/2.5/lib/sqlite3-Controlling-Transactions.html
def begin(using=None):
    """
    begin() and commit() let you explicitly specify an SQL transaction.
    Be sure to call commit() after you call begin().
    """
    get_db(using).ctx.b_commit = False


def commit(using=None):
    """
    begin() and commit() let you explicitly specify an SQL transaction.
    Be sure to call commit() after you call begin().
    """
    try:
        get_db(using).conn.commit()
    finally:
        get_db(using).ctx.b_commit = True


def rollback(using=None):
    """
    begin() and rollback() let you explicitly specify an SQL transaction.
    Be sure to call rollback() after you call begin().
    """
    try:
        get_db(using).conn.rollback()
    finally:
        get_db(using).ctx.b_commit = True
