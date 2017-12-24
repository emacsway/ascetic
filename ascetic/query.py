import copy
import operator
from functools import reduce, partial
from sqlbuilder import smartsql
from ascetic.exceptions import ObjectDoesNotExist
from ascetic.relations import Relation, ForeignKey, OneToOne, OneToMany
from ascetic.signals import field_mangling, column_mangling
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

    def get_fields(self, prefix=None):
        return self._mapper.get_sql_fields(prefix)

    def get_field(self, name):
        if type(name) == tuple:
            return smartsql.CompositeExpr(*(self.get_field(k) for k in name))

        parts = name.split(smartsql.LOOKUP_SEP, 1)
        name = self.__mangle_field(parts[0])

        if name == 'pk':
            name = self._mapper.pk
        elif isinstance(self._mapper.relations.get(name, None), Relation):
            relation = self._mapper.relations.get(name)
            related_alias = relation.related_mapper.sql_table.as_(next(smartsql.auto_name))
            return AutoJoinedTable(
                related_alias,
                smartsql.InnerJoin(None, related_alias, relation.get_join_where(self, related_alias))
            ).f
            # name = self._mapper.relations.get(name).field

        if type(name) == tuple:
            if len(parts) > 1:
                # FIXME: "{}_{}".format(alias, name) ???
                raise Exception("Can't set single alias for multiple fields of composite key {}.{}".format(self.model, name))
            return smartsql.CompositeExpr(*(self.get_field(k) for k in name))

        if name in self._mapper.fields:
            name = self._mapper.fields[name].column

        parts[0] = self.__mangle_column(name)
        return super(Table, self).get_field(smartsql.LOOKUP_SEP.join(parts))

    def __mangle_field(self, name):
        results = field_mangling.send(sender=self, field=name, mapper=self._mapper)
        results = [i[1] for i in results if i[1]]
        if results:
            # response in format tuple(priority: int, mangled_field_name: str)
            results.sort(key=lambda x: x[0], reverse=True)  # Sort by priority
            return results[0][1]
        return name

    def __mangle_column(self, column):
        results = column_mangling.send(sender=self, column=column, mapper=self._mapper)
        results = [i[1] for i in results if i[1]]
        if results:
            # response in format tuple(priority: int, mangled_column_name: str)
            results.sort(key=lambda x: x[0], reverse=True)  # Sort by priority
            return results[0][1]
        return column


@factory.register
class TableAlias(smartsql.TableAlias, Table):
    @property
    def _mapper(self):
        return getattr(self._table, '_mapper', None)  # self._table can be a subquery


class AutoJoinedTable(Table):
    def __init__(self, delegate, auto_join):
        self.m_delegate__ = delegate
        self.m_auto_join__ = auto_join

        smartsql.Table.__init__(self, None)
        if isinstance(delegate, smartsql.Table):
            for f in delegate._fields.values():
                self._append_field(copy.copy(f))

    @property
    def _mapper(self):
        return getattr(self.m_delegate__, '_mapper', None)  # self._table can be a subquery


@smartsql.compile.when(AutoJoinedTable)
def compile_autojoinedtable(compile, expr, state):
    if (expr.m_auto_join__ not in state.auto_join_tables):
        state.auto_join_tables.append(expr.m_auto_join__)
    compile(expr.m_delegate__, state)


class Result(smartsql.Result):
    """Result adapted for table."""

    def __init__(self, mapper, db):
        """
        :type mapper: ascetic.mappers.Mapper
        :type db: ascetic.interfaces.IDatabase
        """
        self.mapper = mapper
        self._prefetch = {}
        self._select_related = {}
        self._is_base = True
        self._map = default_map
        self._cache = None  # empty list also can be a cached result, so, using None instead of empty list
        self._db = db

    def __len__(self):
        self.fill_cache()
        return len(self._cache)

    def __iter__(self):
        self.fill_cache()
        return iter(self._cache)

    def __getitem__(self, key):
        if self._cache:
            return self._cache[key]
        elif isinstance(key, integer_types):
            self._query = super(Result, self).__getitem__(key)
            try:
                return list(self)[0]
            except IndexError:
                raise ObjectDoesNotExist
        else:
            return super(Result, self).__getitem__(key)

    def execute(self):
        """Implementation of query execution"""
        return self._db.execute(self._query)

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
        elif self._cache is None:
            self._cache = list(self.iterator())
            self.populate_prefetch()

    def iterator(self):
        """Iterator"""
        cursor = self.execute()
        fields = tuple(f[0] for f in cursor.description)

        if isinstance(self._map, type):
            map_row = self._map(self)
        else:
            map_row = partial(self._map, result=self, state={})

        for row in cursor.fetchall():
            yield map_row(row=zip(fields, row))

    def db(self, db=None):
        """
        :type db: ascetic.interfaces.IDatabase or None
        :rtype: ascetic.interfaces.IDatabase
        """
        if db is None:
            return self._db
        self._db = db
        return self._query

    def is_base(self, value=None):
        if value is None:
            return self._is_base
        self._is_base = value
        return self._query

    def map(self, map):
        """Sets map."""
        c = self
        c._map = map
        return c._query

    def prefetch(self, *a, **kw):
        """Prefetch relations"""
        relations = self.mapper.relations
        if a and not a[0]:  # .prefetch(False)
            self._prefetch = {}
        else:
            self._prefetch = copy.copy(self._prefetch)
            self._prefetch.update(kw)
            self._prefetch.update({i: relations[i].related_query for i in a})
        return self._query

    def populate_prefetch(self):
        relations = self.mapper.relations
        for key, query in self._prefetch.items():
            relation = relations[key]
            preset_relation = RelationPresetter(relation)
            # recursive handle prefetch
            cond = reduce(operator.or_, (relation.get_related_where(obj) for obj in self._cache))
            query = query.where(cond)
            for obj in self._cache:
                for prefetched_obj in query:
                    if relation.get_value(obj) == relation.get_related_value(prefetched_obj):
                        preset_relation(obj, related_obj=prefetched_obj)


class RelationPresetter(object):
    def __new__(cls, relation):
        if isinstance(relation, ForeignKey):
            return object.__new__(ForeignKeyPresetter)
        elif isinstance(relation, OneToOne):
            return object.__new__(OneToOnePresetter)
        elif isinstance(relation, OneToMany):
            return object.__new__(OneToManyPresetter)
        else:
            raise NotImplementedError(relation)

    def __init__(self, relation):
        """
        :type relation: ascetic.relations.Relation
        """
        self._relation = relation

    def __call__(self, obj, related_obj):
        raise NotImplementedError

    @property
    def name(self):
        return self._relation.name

    @property
    def related_name(self):
        return self._relation.related_name

    @staticmethod
    def set_value(obj, attr_name, related_obj):
        setattr(obj, attr_name, related_obj)

    @staticmethod
    def append_value(obj, attr_name, related_item):
        query = getattr(obj, attr_name)
        if query.result._cache is None:
            query.result._cache = []
        query.result._cache.append(related_item)


class ForeignKeyPresetter(RelationPresetter):
    def __call__(self, obj, related_obj):
        self.set_value(obj, self.name, related_obj)
        self.append_value(related_obj, self.related_name, obj)


class OneToOnePresetter(RelationPresetter):
    def __call__(self, obj, related_obj):
        self.set_value(obj, self.name, related_obj)
        self.set_value(related_obj, self.related_name, obj)


class OneToManyPresetter(RelationPresetter):
    def __call__(self, obj, related_obj):
        if not hasattr(obj, self.name):
            setattr(obj, self.name, [])
        self.append_value(obj, self.name, related_obj)
        self.set_value(related_obj, self.related_name, obj)


def default_map(result, row, state):
    return result.mapper.load(row, result.db(), from_db=True)


class SelectRelatedMap(object):

    def __init__(self, result):
        self._result = result
        self._state = {}

    def __call__(self, row):
        models = [self._result.mapper.model]
        relations = self._result._select_related
        for rel in relations:
            models.append(rel.related_model)
        rows = self._get_model_rows(models, row)
        objs = self._get_objects(models, rows)
        self._build_relations(relations, objs)
        return objs

    def _get_model_rows(self, models, row):
        rows = []  # There can be multiple the same models, so, using dict instead of model
        start = 0
        for model in models:
            mapper = self._result.mapper.get_mapper(model)
            length = len(mapper.get_sql_fields())
            rows.append(row[start:length])
            start += length
        return rows

    def _get_objects(self, models, rows):
        objs = []
        for model, model_row in zip(models, rows):
            mapper = self._result.mapper.get_mapper(model)
            pk = to_tuple(mapper.pk)
            pk_columns = tuple(mapper.fields[k].columns for k in pk)
            model_row_dict = dict(model_row)
            pk_values = tuple(model_row_dict[k] for k in pk_columns)
            key = (model, pk_values)
            if key not in self._state:
                self._state[key] = mapper.load(model_row, self._result.db(), from_db=True)
            objs.append(self._state[key])
        return objs

    def _build_relations(self, relations, objs):
        for i, relation in enumerate(relations):
            obj, related_obj = objs[i], objs[i + 1]
            RelationPresetter(relation)(obj, related_obj)
