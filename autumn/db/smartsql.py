"""
sqlbuilder integration, https://bitbucket.org/evotech/sqlbuilder
"""

from __future__ import absolute_import, unicode_literals
from sqlbuilder import smartsql
from autumn import settings
from autumn.db.query import Query
from autumn.models import Model

SMARTSQL_ALIAS = getattr(settings, 'SQLBUILDER_SMARTSQL_ALIAS', 'ss')


class classproperty(object):
    """Class property decorator"""
    def __init__(self, getter):
        self.getter = getter

    def __get__(self, instance, owner):
        return self.getter(owner)


class QS(smartsql.QS):
    """Query Set adapted."""

    def __len__(self):
        """Returns length or list."""
        return len(self.execute())

    def count(self):
        """Returns length or list."""
        return len(self.execute())

    def __iter__(self):
        """Returns iterator."""
        return iter(self.execute())

    def iterator(self):
        """Returns iterator."""
        return self.execute().iterator()

    def __getitem__(self, key):
        """Returns sliced self or item."""
        return self.execute()[key]

    def execute(self):
        """Implementation of query execution"""
        if self._action in ('select', 'count', ):
            return Query(model=self.model).raw(
                smartsql.sqlrepr(self), smartsql.sqlparams(self)
            )
        else:
            return Query.raw_sql(
                smartsql.sqlrepr(self),
                smartsql.sqlparams(self),
                self.model.db
            )

    def result(self):
        """Result"""
        if self._action in ('select', 'count', ):
            return self
        return self.execute()


class Table(smartsql.Table):
    """Table class"""

    def get_fields(self, prefix=None):
        """Returns field list."""
        if prefix is None:
            prefix = self
        result = []
        for f in self.model._fields:
            result.append(smartsql.Field(f, prefix))
        return result

    @property
    def qs(self):
        r = QS(self).fields(self.get_fields())
        r.base_table = self
        r.model = self.model
        return r


def make_table(cls):
    """Table factory"""
    t = Table(cls.Meta.table)
    t.model = cls
    return t


@classproperty
def ss(cls):
    if getattr(cls, '_{0}'.format(SMARTSQL_ALIAS), None) is None:
        setattr(cls, '_{0}'.format(SMARTSQL_ALIAS), make_table(cls))
    return getattr(cls, '_{0}'.format(SMARTSQL_ALIAS))


def smartsql_init():
    setattr(Model, SMARTSQL_ALIAS, ss)
