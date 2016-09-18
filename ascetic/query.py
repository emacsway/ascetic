import copy
import operator
from functools import reduce, partial
from sqlbuilder import smartsql
from ascetic.databases import databases
from ascetic.exceptions import ObjectDoesNotExist
from ascetic.mappers import mapper_registry
from ascetic.relations import Relation, ForeignKey, OneToOne, OneToMany
from ascetic.utils import to_tuple

factory = copy.copy(smartsql.factory)


try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)


@factory.register
class Table(smartsql.Table):

    def __init__(self, mapper, *args, **kwargs):
        """
        :type mapper: ascetic.mappers.Mapper
        """
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


class Result(smartsql.Result):
    """Result adapted for table."""

    def __init__(self, mapper):
        """
        :type mapper: ascetic.mappers.Mapper
        """
        self.mapper = mapper
        self._prefetch = {}
        self._select_related = {}
        self._is_base = True
        self._mapping = default_mapping
        self._using = mapper._using
        self._cache = None

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
        if isinstance(self._mapping, type):
            map_row = self._mapping(self)
        else:
            map_row = partial(self._mapping, result=self, state={})
        for row in cursor.fetchall():
            yield map_row(row=zip(fields, row))

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
        relations = self.mapper.relations
        if a and not a[0]:  # .prefetch(False)
            self._prefetch = {}
        else:
            self._prefetch = copy.copy(self._prefetch)
            self._prefetch.update(kw)
            self._prefetch.update({i: relations[i].rel_query for i in a})
        return self._query

    def populate_prefetch(self):
        relations = self.mapper.relations
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
    return result.mapper.load(row, from_db=True)


class SelectRelatedMapping(object):

    def __init__(self, result):
        self._result = result
        self._state = {}

    def _get_model_rows(self, models, row):
        rows = []
        start = 0
        for m in models:
            length = len(m.s.get_fields())
            rows.append(row[start:length])
            start += length
        return rows

    def _get_objects(self, models, rows):
        objs = []
        for model, model_row in zip(models, rows):
            mapper = mapper_registry[model]
            pk = to_tuple(mapper.pk)
            pk_columns = tuple(mapper.fields[k].columns for k in pk)
            model_row_dict = dict(model_row)
            pk_values = tuple(model_row_dict[k] for k in pk_columns)
            key = (model, pk_values)
            if key not in self._state:
                self._state[key] = mapper.load(model_row, from_db=True)
            objs.append(self._state[key])
        return objs

    def _build_relations(self, relations, objs):
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

    def __call__(self, row):
        models = [self._result.mapper.model]
        relations = self._result._select_related
        for rel in relations:
            models.append(rel.rel_model)
        rows = self._get_model_rows(models, row)
        objs = self._get_objects(models, rows)
        self._build_relations(relations, objs)
        return objs[0]
