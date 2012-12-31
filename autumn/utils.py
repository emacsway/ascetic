from __future__ import absolute_import, unicode_literals

# Autumn ORM
from autumn.db.query import Query


"""
Convenience functions for the Autumn ORM.
"""


def table_exists(db, table_name):
    """
    Given an Autumn model, check to see if its table exists.
    """
    try:
        s_sql = "SELECT * FROM {0} LIMIT 1;".format(table_name)
        Query.raw_sql(s_sql, db=db)
    except Exception:
        return False

    # if no exception, the table exists and we are done
    return True


def create_table(db, s_create_sql):
    """
    Create a table for an Autumn class.
    """
    Query.begin(db=db)
    Query.raw_sqlscript(s_create_sql, db=db)
    Query.commit(db=db)


def create_table_if_needed(db, table_name, s_create_sql):
    """
    Check to see if an Autumn class has its table created; create if needed.
    """
    if not table_exists(db, table_name):
        create_table(db, s_create_sql)


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
