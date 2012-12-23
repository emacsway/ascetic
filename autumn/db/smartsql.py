"""
sqlbuilder integration, https://bitbucket.org/evotech/sqlbuilder
"""

from __future__ import absolute_import, unicode_literals
from sqlbuilder import smartsql
from autumn import settings
from autumn.db.query import Query
from autumn.db import relations
from autumn.models import Model
from autumn.db.connection import connections

SMARTSQL_ALIAS = getattr(settings, 'SQLBUILDER_SMARTSQL_ALIAS', 'ss')

SMARTSQL_DIALECTS = {
    'sqlite3': 'sqlite',
    'mysql': 'mysql',
    'postgresql': 'postgres',
    'postgresql_psycopg2': 'postgres',
    'postgis': 'postgres',
    'oracle': 'oracle',
}


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

    def dialect(self):
        engine = connections[self.model.using].engine
        return SMARTSQL_DIALECTS.get(engine, engine)

    def sqlrepr(self):
        return smartsql.sqlrepr(self, self.dialect())

    def sqlparams(self):
        return smartsql.sqlparams(self)

    def execute(self):
        """Implementation of query execution"""
        if self._action in ('select', 'count', ):
            return Query(model=self.model).raw(
                self.sqlrepr(),
                *self.sqlparams()
            )
        else:
            return Query.raw_sql(
                self.sqlrepr(),
                self.sqlparams(),
                self.model.using
            )

    def result(self):
        """Result"""
        if self._action in ('select', 'count', ):
            return self
        return self.execute()


class Table(smartsql.Table):
    """Table class"""

    def __init__(self, model, *args, **kwargs):
        """Constructor"""
        super(Table, self).__init__(model.Meta.table, *args, **kwargs)
        self.model = model
        self.qs = kwargs.pop('qs', QS(self).fields(self.get_fields()))
        self.qs.base_table = self
        self.qs.model = self.model

    def get_fields(self, prefix=None):
        """Returns field list."""
        if prefix is None:
            prefix = self
        result = []
        for f in self.model._fields:
            result.append(smartsql.Field(f, prefix))
        return result

    def __getattr__(self, name):
        """Added some specific functional."""
        if name[0] == '_':
            raise AttributeError
        parts = name.split(smartsql.LOOKUP_SEP, 1)
        result = {'field': parts[0], }
        settings.send_signal(signal='field_conversion', sender=self, result=result, field=parts[0], model=self.model)
        parts[0] = result['field']
        return super(Table, self).__getattr__(smartsql.LOOKUP_SEP.join(parts))

class RelationQSMixIn(object):

    def get_qs(self):
        return self.qs and self.qs.clone() or getattr(self.model, SMARTSQL_ALIAS).qs.clone()

    def filter(self, *a, **kw):
        qs = self.get_qs()
        t = getattr(self.model, SMARTSQL_ALIAS)
        for fn, param in kw.items():
            f = getattr(t, fn)
            qs = qs.where(f == param)
        return qs


class Relation(RelationQSMixIn, relations.Relation):
    pass


class ForeignKey(RelationQSMixIn, relations.ForeignKey):
    pass


class OneToMany(RelationQSMixIn, relations.OneToMany):
    pass


@classproperty
def ss(cls):
    if getattr(cls, '_{0}'.format(SMARTSQL_ALIAS), None) is None:
        setattr(cls, '_{0}'.format(SMARTSQL_ALIAS), Table(cls))
    return getattr(cls, '_{0}'.format(SMARTSQL_ALIAS))


def smartsql_init():
    setattr(Model, SMARTSQL_ALIAS, ss)
