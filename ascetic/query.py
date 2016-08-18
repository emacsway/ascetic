import copy
import operator
from functools import reduce
from sqlbuilder import smartsql
from .databases import databases
from .mappers import to_tuple, ObjectDoesNotExist
from .relations import Relation, ForeignKey, OneToOne, OneToMany

factory = copy.copy(smartsql.factory)


try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)


class Result(smartsql.Result):
    """Result adapted for table."""

    _mapper = None
    _raw = None
    _cache = None
    _using = 'default'

    def __init__(self, mapper):
        self._prefetch = {}
        self._select_related = {}
        self._is_base = True
        self._mapping = default_mapping
        self._mapper = mapper
        self._using = mapper._using

    def __len__(self):
        self.fill_cache()
        return len(self._cache)

    def __iter__(self):
        self.fill_cache()
        return iter(self._cache)

    def __getitem__(self, key):
        if self._cache:
            return self._cache[key]
        if isinstance(key, integer_types):
            self._query = super(Result, self).__getitem__(key)
            try:
                return list(self)[0]
            except IndexError:
                raise ObjectDoesNotExist
        return super(Result, self).__getitem__(key)

    def execute(self):
        """Implementation of query execution"""
        return self.db.execute(self._query)

    insert = update = delete = execute

    def select(self):
        return self

    def count(self):
        if self._cache is not None:
            return len(self._cache)
        return self.execute().fetchone()[0]

    def clone(self):
        c = smartsql.Result.clone(self)
        c._cache = None
        c._is_base = False
        return c

    def fill_cache(self):
        if self.is_base():
            raise Exception('You should clone base queryset before query.')
        if self._cache is None:
            self._cache = list(self.iterator())
            self.populate_prefetch()

    def iterator(self):
        """Iterator"""
        cursor = self.execute()
        fields = tuple(f[0] for f in cursor.description)
        state = {}
        for row in cursor.fetchall():
            yield self._mapping(self, zip(fields, row), state)

    def using(self, alias=False):
        if alias is None:
            return self._using
        if alias is not False:
            self._using = alias
        return self._query

    def is_base(self, value=None):
        if value is None:
            return self._is_base
        self._is_base = value
        return self._query

    @property
    def db(self):
        return databases[self._using]

    def map(self, mapping):
        """Sets mapping."""
        c = self
        c._mapping = mapping
        return c._query

    def prefetch(self, *a, **kw):
        """Prefetch relations"""
        relations = self._mapper.relations
        if a and not a[0]:  # .prefetch(False)
            self._prefetch = {}
        else:
            self._prefetch = copy.copy(self._prefetch)
            self._prefetch.update(kw)
            self._prefetch.update({i: relations[i].rel_query for i in a})
        return self._query

    def populate_prefetch(self):
        relations = self._mapper.relations
        for key, q in self._prefetch.items():
            rel = relations[key]
            # recursive handle prefetch

            cond = reduce(operator.or_, (rel.get_rel_where(obj) for obj in self._cache))
            q = q.where(cond)
            rows = list(q)
            for obj in self._cache:
                val = [i for i in rows if rel.get_rel_value(i) == rel.get_value(obj)]
                if isinstance(rel, (ForeignKey, OneToOne)):
                    val = val[0] if val else None
                    if val and isinstance(rel, OneToOne):
                        setattr(val, rel.rel_name, obj)
                elif isinstance(rel, OneToMany):
                    for i in val:
                        setattr(i, rel.rel_name, obj)
                setattr(obj, key, val)


def default_mapping(result, row, state):
    return result._mapper.load(row, from_db=True)


class SelectRelatedMapping(object):

    def get_model_rows(self, models, row):
        rows = []
        start = 0
        for m in models:
            length = len(m.s.get_fields())
            rows.append(row[start:length])
            start += length
        return rows

    def get_objects(self, models, rows, state):
        objs = []
        for model, model_row in zip(models, rows):
            pk = to_tuple(model._mapper.pk)
            pk_columns = tuple(model._mapper.fields[k].columns for k in pk)
            model_row_dict = dict(model_row)
            pk_values = tuple(model_row_dict[k] for k in pk_columns)
            key = (model, pk_values)
            if key not in state:
                state[key] = model._mapper.load(model_row, from_db=True)
            objs.append(state[key])
        return objs

    def build_relations(self, relations, objs):
        for i, rel in enumerate(relations):
            obj, rel_obj = objs[i], objs[i + 1]
            name = rel.name
            rel_name = rel.rel_name
            if isinstance(rel, (ForeignKey, OneToOne)):
                setattr(obj, name, rel_obj)
                if not hasattr(rel_obj, rel_name):
                    setattr(rel_obj, rel_name, [])
                getattr(rel_obj, rel_name).append[obj]
            elif isinstance(rel, OneToMany):
                if not hasattr(obj, name):
                    setattr(obj, name, [])
                getattr(obj, name).append[rel_obj]
                setattr(rel_obj, rel_name, obj)

    def __call__(self, result, row, state):
        models = [result.model]
        relations = result._select_related
        for rel in relations:
            models.append(rel.rel_model)
        rows = self.get_model_rows(models, row)
        objs = self.get_objects(models, rows, state)
        self.build_relations(relations, objs)
        return objs[0]


@factory.register
class Table(smartsql.Table):

    def __init__(self, mapper, *args, **kwargs):
        super(Table, self).__init__(mapper.db_table, *args, **kwargs)
        self._mapper = mapper

    @property
    def q(self):
        return self._mapper.query

    @property
    def qs(self):
        smartsql.warn('Table.qs', 'Table.q')
        return self._mapper.query

    def get_fields(self, prefix=None):
        return self._mapper.get_sql_fields()

    def get_field(self, name):
        parts = name.split(smartsql.LOOKUP_SEP, 1)
        field = parts[0]
        # result = {'field': field, }
        # field_conversion.send(sender=self, result=result, field=field, model=self.model)
        # field = result['field']

        if field == 'pk':
            field = self._mapper.pk
        elif isinstance(self._mapper.relations.get(field, None), Relation):
            field = self._mapper.relations.get(field).field

        if type(field) == tuple:
            if len(parts) > 1:
                # FIXME: "{}_{}".format(alias, field_name) ???
                raise Exception("Can't set single alias for multiple fields of composite key {}.{}".format(self.model, name))
            return smartsql.CompositeExpr(*(self.get_field(k) for k in field))

        if field in self._mapper.fields:
            field = self._mapper.fields[field].column
        parts[0] = field
        return super(Table, self).get_field(smartsql.LOOKUP_SEP.join(parts))


@factory.register
class TableAlias(smartsql.TableAlias, Table):
    @property
    def _mapper(self):
        return getattr(self._table, '_mapper', None)  # Can be subquery
