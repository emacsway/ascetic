from __future__ import absolute_import, unicode_literals
from autumn.db.query import execute, executescript, begin, commit
"""
Convenience functions for the Autumn ORM.
"""


def table_exists(table_name, using=None):
    """
    Given an Autumn model, check to see if its table exists.
    """
    try:
        sql = "SELECT * FROM {0} LIMIT 1;".format(table_name)
        execute(sql, using=using)
    except Exception:
        return False

    # if no exception, the table exists and we are done
    return True


def create_table(create_sql, using=None):
    """
    Create a table for an Autumn class.
    """
    begin(using=using)
    executescript(create_sql, using=using)
    commit(using=using)


def create_table_if_needed(table_name, create_sql, using=None):
    """
    Check to see if an Autumn class has its table created; create if needed.
    """
    if not table_exists(table_name, using=using):
        create_table(create_sql, using=using)


# examples of usage:
#
# class FooClass(object):
#     db = autumn.util.AutoConn("foo.db")
#
# _create_sql = "_create_sql = """\
# DROP TABLE IF EXISTS bar;
# CREATE TABLE bar (
#     id INTEGER PRIMARY KEY,
#     value VARCHAR(128) NOT NULL,
#     UNIQUE (value));
# CREATE INDEX idx_bar0 ON bar (value);"""
#
# autumn.util.create_table_if_needed(FooClass.db, "bar", _create_sql)
#
# class Bar(FooClass, Model):
#    ...standard Autumn class stuff goes here...
