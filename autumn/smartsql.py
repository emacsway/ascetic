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
    prefix_result = False
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

        fields = []
        for f in cursor.description:
            fn = f[0]
            c = 2
            while fn in fields:
                fn = fn + str(2)
                c += 1
            fields.append(fn)

        if self.prefix_result:
            # TODO: variant init_fields = ((model1, model_field_list1), (model2, model_field_list2), ...)?
            init_fields = self.get_init_fields()
            if len(fields) == len(init_fields):
                fields = init_fields

        for row in cursor.fetchall():
            data = dict(list(zip(fields, row)))
            if self.model:
                # obj = self.model(*row)
                obj = self.model(**data)
                obj._new_record = False
                yield obj
            else:
                yield data

    def get_init_fields(self):
        """Returns list of fields what was passed to query."""
        init_fields = []
        for f in self._fields:
            if isinstance(f, smartsql.F):
                if isinstance(f._prefix, Table) and f._prefix.model == self.model:
                    init_fields.append(f._name)
                    continue
            init_fields.append('__'.join(self.sqlrepr(f).replace('`', '').replace('"', '').split('.')))
        return init_fields

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

    def sqlrepr(self, expr=None):
        return smartsql.sqlrepr(expr or self, self.dialect())

    def sqlparams(self, expr=None):
        return smartsql.sqlparams(expr or self)

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

    def as_union(self):
        return UnionQuerySet(self)


class UnionQuerySet(smartsql.UnionQuerySet, QS):
    """Union query class"""
    def __init__(self, qs):
        super(UnionQuerySet, self).__init__(qs)
        self.model = qs.model
        self.using = qs.using
        self.base_table = qs.base_table


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
        from . import relations
        if name[0] == '_':
            raise AttributeError
        parts = name.split(smartsql.LOOKUP_SEP, 1)
        result = {'field': parts[0], }
        settings.send_signal(signal='field_conversion', sender=self, result=result, field=parts[0], model=self.model)
        parts[0] = result['field']
        if parts[0] == 'pk':
            parts[0] = self.model._meta.pk
        if isinstance(self.model._meta.relations.get(parts[0], None), relations.ForeignKey):
            parts[0] = self.model._meta.relations.get(parts[0]).field
        return super(Table, self).__getattr__(smartsql.LOOKUP_SEP.join(parts))

    def as_(self, alias):
        return TableAlias(alias, self)


class TableAlias(smartsql.TableAlias, Table):
    """Table alias class"""
    @property
    def model(self):
        return self.table.model
