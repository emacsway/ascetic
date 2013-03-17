"""
sqlbuilder integration, https://bitbucket.org/evotech/sqlbuilder
"""

from __future__ import absolute_import, unicode_literals
from sqlbuilder import smartsql
from . import settings
from .connections import get_db

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)

SMARTSQL_DIALECTS = {
    'sqlite3': 'sqlite',
    'mysql': 'mysql',
    'postgresql': 'postgres',
    'postgresql_psycopg2': 'postgres',
    'postgis': 'postgres',
    'oracle': 'oracle',
}


def qn(name, using='default'):
    """Quotes DB name"""
    engine = get_db(using).engine
    return smartsql.qn(name, SMARTSQL_DIALECTS.get(engine, engine))


class classproperty(object):
    """Class property decorator"""
    def __init__(self, getter):
        self.getter = getter

    def __get__(self, instance, owner):
        return self.getter(owner)


class QS(smartsql.QS):
    """Query Set adapted."""

    _cache = None
    using = 'default'

    def raw(self, sql, *params):
        self = self.clone()
        self._sql = sql
        self._params = params
        return self

    def clone(self):
        self = super(QS, self).clone()
        self._cache = None
        return self

    def __len__(self):
        """Returns length or list."""
        self.fill_cache()
        return len(self._cache)

    def count(self):
        """Returns length or list."""
        if self._cache:
            return len(self._cache)
        qs = self.order_by(reset=True)
        sql = "SELECT COUNT(1) as count_value FROM ({0}) as count_list".format(
            qs.sqlrepr()
        )
        return self._execute(sql, *qs.sqlparams()).fetchone()[0]

    def fill_cache(self):
        if self._cache is None:
            self._cache = list(self.iterator())
        return self

    def __iter__(self):
        """Returns iterator."""
        self.fill_cache()
        return iter(self._cache)

    def iterator(self):
        """iterator"""
        if self._sql:
            sql = self._sql
            if self._limit:
                sql = ' '.join([sql, smartsql.sqlrepr(self._limit, self.dialect())])
            cursor = self._execute(sql, *self._params)
        else:
            cursor = self._execute(self.sqlrepr(), *self.sqlparams())
        fields = [f[0] for f in cursor.description]
        for row in cursor.fetchall():
            data = dict(list(zip(fields, row)))
            if self.model:
                # obj = self.model(*row)
                obj = self.model(**data)
                obj._new_record = False
                yield obj
            else:
                yield data

    def __getitem__(self, key):
        """Returns sliced self or item."""
        if self._cache:
            return self._cache[key]
        if isinstance(key, integer_types):
            self = self.clone()
            self = super(QS, self).__getitem__(key)
            return list(self)[0]
        return super(QS, self).__getitem__(key)

    def dialect(self):
        engine = get_db(self.using).engine
        return SMARTSQL_DIALECTS.get(engine, engine)

    def sqlrepr(self):
        return smartsql.sqlrepr(self, self.dialect())

    def sqlparams(self):
        return smartsql.sqlparams(self)

    def execute(self):
        """Implementation of query execution"""
        if self._action in ('select', 'count', ):
            return self
        else:
            return self._execute(self.sqlrepr(), *self.sqlparams())

    def _execute(self, sql, *params):
        return get_db(self.using).execute(sql, params)

    def result(self):
        """Result"""
        if self._action in ('select', 'count', ):
            return self
        return self.execute()

    def begin(self):
        return get_db(self.using).begin()

    def commit(self):
        return get_db(self.using).commit()

    def rollback(self):
        return get_db(self.using).rollback()


class Table(smartsql.Table):
    """Table class"""

    def __init__(self, model, *args, **kwargs):
        """Constructor"""
        super(Table, self).__init__(model._meta.db_table, *args, **kwargs)
        self.model = model
        self.qs = kwargs.pop('qs', QS(self).fields(self.get_fields()))
        self.qs.base_table = self
        self.qs.model = self.model
        self.qs.using = self.model._meta.using

    def get_fields(self, prefix=None):
        """Returns field list."""
        if prefix is None:
            prefix = self
        result = []
        for f in self.model._meta.fields:
            result.append(smartsql.Field(f, prefix))
        return result

    def __getattr__(self, name):
        """Added some specific functional."""
        from .relations import ForeignKey
        if name[0] == '_':
            raise AttributeError
        parts = name.split(smartsql.LOOKUP_SEP, 1)
        result = {'field': parts[0], }
        settings.send_signal(signal='field_conversion', sender=self, result=result, field=parts[0], model=self.model)
        parts[0] = result['field']
        if isinstance(self.model.__dict__.get(parts[0], None), ForeignKey):
            getattr(self.model, parts[0])  # call ForeignKey.set_up()
            parts[0] = self.model.__dict__.get(parts[0]).field
        return super(Table, self).__getattr__(smartsql.LOOKUP_SEP.join(parts))


class RelationQSMixIn(object):

    def get_qs(self):
        return self.qs and self.qs.clone() or self.model.ss.qs.clone()

    def filter(self, *a, **kw):
        qs = self.get_qs()
        t = self.model.ss
        for fn, param in kw.items():
            f = smartsql.Field(fn, t)
            qs = qs.where(f == param)
        return qs
