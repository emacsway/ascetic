from __future__ import absolute_import
import re
import copy
import weakref
import operator
import collections
from functools import reduce

from sqlbuilder import smartsql
from .databases import databases
from .signals import pre_save, post_save, pre_delete, post_delete, pre_init, post_init, class_prepared
from .utils import classproperty, cached_property
from .validators import ValidationError

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)

cr = copy.copy(smartsql.cr)


def to_tuple(val):
    return val if type(val) == tuple else (val,)


def is_model_instance(obj):
    return obj.__class__ in model_registry.values()


def is_model(cls):
    return cls in model_registry.values()


class OrmException(Exception):
    pass


class ModelNotRegistered(OrmException):
    pass


class MapperNotRegistered(OrmException):
    pass


class ObjectDoesNotExist(OrmException):
    pass


class BaseRegistry(dict):

    exception_class = OrmException

    def __getitem__(self, key):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            raise self.exception_class("""{} is not registered in {}""".format(key, self.keys()))

    def __call__(self, key):
        return self[key]

    def get(self, key, default=None):
        try:
            return self[key]
        except self.exception_class:
            return default


class ModelRegistry(BaseRegistry):
    exception_class = ModelNotRegistered

model_registry = get_model = ModelRegistry()


class MapperRegistry(BaseRegistry):
    exception_class = MapperNotRegistered

mapper_registry = get_mapper = MapperRegistry()


class WeakCache(object):

    def __init__(self, size=1000):
        self._order = []
        self._size = size

    def add(self, value):
        self._order.append(value)
        if len(self._order) > self._size:
            self._order.pop(0)

    def touch(self, value):
        try:
            self._order.remove(value)
        except IndexError:
            pass
        self._order.append(value)

    def remove(self, value):
        try:
            self._order.remove(value)
        except IndexError:
            pass

    def clear(self):
        del self._order[:]

    def set_size(self, size):
        self._size = size


class IdentityMap(object):
    # TODO: bind to connect or store

    READ_UNCOMMITTED = 0  # IdentityMap is disabled
    READ_COMMITTED = 1  # IdentityMap is disabled
    REPEATABLE_READ = 2  # Prevent repeated DB-query only for existent objects
    SERIALIZABLE = 3  # Prevent repeated DB-query for both, existent and nonexistent objects

    INFLUENCING_LEVELS = (REPEATABLE_READ, SERIALIZABLE)

    # _isolation_level = READ_UNCOMMITTED  # Disabled currently
    _isolation_level = SERIALIZABLE

    class Nonexistent(object):
        pass

    def __new__(cls, alias='default', *args, **kwargs):
        if not hasattr(databases[alias], 'identity_map'):
            self = databases[alias].identity_map = object.__new__(cls)
            self._cache = WeakCache()
            self._alive = weakref.WeakValueDictionary()
        return databases[alias].identity_map

    def add(self, key, value=None):
        if self._isolation_level not in self.INFLUENCING_LEVELS:
            return
        if value is None:
            if self._isolation_level != self.SERIALIZABLE:
                return
            value = self.Nonexistent()
        self._cache.add(value)
        self._alive[key] = value

    def get(self, key):
        if self._isolation_level not in self.INFLUENCING_LEVELS:
            raise KeyError
        value = self._alive[key]
        self._cache.touch(value)
        if value.__class__ == self.Nonexistent:
            if self._isolation_level != self.SERIALIZABLE:
                raise KeyError
            raise ObjectDoesNotExist
        return value

    def remove(self, key):
        try:
            value = self._alive[key]
            self._cache.remove(value)
            del self._alive[key]
        except KeyError:
            pass

    def clear(self):
        self._cache.clear()
        self._alive.clear()

    def exists(self, key):
        if self._isolation_level not in self.INFLUENCING_LEVELS:
            return False
        return key in self._alive

    def set_isolation_level(self, level):
        self._isolation_level = level

    def enable(self):
        if hasattr(self, '_last_isolation_level'):
            self._isolation_level = self._last_isolation_level
            del self._last_isolation_level

    def disable(self):
        if not hasattr(self, '_last_isolation_level'):
            self._last_isolation_level = self._isolation_level
            self._isolation_level = self.READ_UNCOMMITTED


class Field(object):

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


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


class Mapper(object):

    pk = 'id'
    default_using = 'default'
    _using = 'default'
    abstract = False
    field_factory = Field
    result_factory = Result

    def __init__(self, model=None):
        if model:
            self.model = model

        if not hasattr(self, 'name'):
            self.name = self._create_default_name(model)

        model_registry[self.name] = model
        mapper_registry[model] = self
        self.declared_fields = self._create_declared_fields(
            model,
            getattr(self, 'map', {}),
            getattr(self, 'defaults', {}),
            getattr(self, 'validations', {}),
            getattr(self, 'declared_fields', {})
        )

        self._prepare_model(model)
        self._inherit(self, (mapper_registry[base] for base in self.model.__bases__ if base in mapper_registry))  # recursive

        if self.abstract:
            return

        self._using = self.default_using

        if not hasattr(self, 'db_table'):
            self.db_table = self._create_default_db_table(model)

        # fileds and columns can be a descriptor for multilingual mapping.
        self.fields = collections.OrderedDict()
        self.columns = collections.OrderedDict()

        for name, field in self.create_fields(self._read_columns(self.db_table, self._using), self.declared_fields).items():
            self.add_field(name, field)

        self.pk = self._read_pk(self.db_table, self._using, self.columns)

        self.sql_table = self._create_sql_table()
        self.base_query = self._create_base_query()
        self.query = self._create_query()

    def _create_default_name(self, model):
        return ".".join((model.__module__, model.__name__))

    def _create_default_db_table(self, model):
        return "_".join([
            re.sub(r"[^a-z0-9]", "", i.lower())
            for i in (self.model.__module__.split(".") + [self.model.__name__, ])
        ])

    def _create_declared_fields(self, model, map, defaults, validations, declared_fields):
        # We don't need depend on the state of instance, to be able to customise, or even reproduce some steps of initialisation.
        # So, we accept all data as arguments.
        result = {}

        for name in model.__dict__:
            field = getattr(model, name, None)
            if isinstance(field, Field):
                result[name] = field

        for name, column in map.items():
            result[name] = self.create_field(name, {'column': column}, declared_fields)

        for name, default in defaults.items():
            result[name] = self.create_field(name, {'default': default}, declared_fields)

        for name, validators in validations.items():
            if not isinstance(validators, (list, tuple)):
                validators = [validators, ]
            result[name] = self.create_field(name, {'validators': validators}, declared_fields)

        return result

    def _read_pk(self, db_table, using, columns):
        db = databases[using]
        if hasattr(db, 'get_pk'):
            pk = tuple(columns[i].name for i in db.get_pk(db_table))
            return pk[0] if len(pk) == 1 else pk
        return self.__class__.pk

    def _read_columns(self, db_table, using):
        db = databases[using]
        schema = db.describe_table(db_table)
        q = db.execute('SELECT * FROM {0} LIMIT 1'.format(db.qn(self.db_table)))
        # See cursor.description http://www.python.org/dev/peps/pep-0249/
        result = []
        for row in q.description:
            column = row[0]
            data = schema.get(column) or {}
            data.update({'column': column, 'type_code': row[1]})
            result.append(data)
        return result

    def create_field(self, name, data, declared_fields=None):
        if declared_fields and name in declared_fields:
            field = copy.deepcopy(declared_fields[name])
            field.__dict__.update(data)
        else:
            field = self.field_factory(**data)
        return field

    def create_fields(self, columns, declared_fields):
        fields = collections.OrderedDict()
        rmap = {field.column: name for name, field in declared_fields.items() if hasattr(field, 'column')}
        for data in columns:
            column_name = data['column']
            name = rmap.get(column_name, column_name)
            fields[name] = self.create_field(name, data, declared_fields)
        for name, field in declared_fields.items():
            if name not in fields:
                fields[name] = self.create_field(name, {'virtual': True}, declared_fields)
        return fields

    def add_field(self, name, field):
        field.name = name
        field._mapper = self
        self.fields[name] = field
        self.columns[field.column] = field

    def _create_sql_table(self):
        return Table(self)

    def _create_base_query(self):
        """For relations."""
        return smartsql.Q(self.sql_table, result=self.result_factory(self)).fields(self.get_sql_fields())

    def _create_query(self):
        """For selection."""
        return smartsql.Q(self.sql_table, result=self.result_factory(self)).fields(self.get_sql_fields())

    def get_sql_fields(self, prefix=None):
        """Returns field list."""
        if prefix is None:
            prefix = self.sql_table
        return [smartsql.Field(f.column, prefix) for f in self.fields.values() if not getattr(f, 'virtual', False)]

    def _do_prepare_model(self, model):
        pass

    def _prepare_model(self, model):
        for name in self.model.__dict__:
            field = getattr(model, name, None)
            if isinstance(field, Field):
                delattr(model, name)

        if getattr(self, 'relationships', None):  # TODO: Give me better name (relationships, references, set_relations, ...)
            for key, rel in self.relationships.items():
                setattr(model, key, rel)

        # TODO: use dir() instead __dict__ to handle relations in abstract classes,
        # add templates support for related_name,
        # support copy.
        # for key, rel in model.__dict__.items():
        for key in dir(model):
            rel = getattr(model, key, None)
            if isinstance(rel, BaseRelation):
                rel = RelationDescriptor(rel)
                setattr(model, key, rel)
            if isinstance(rel, RelationDescriptor) and hasattr(rel.relation, 'setup_related'):
                try:
                    rel.get_bound_relation(model).setup_related()
                except ModelNotRegistered:
                    pass

        for rel_model in model_registry.values():
            for key, rel in mapper_registry[rel_model].relations.items():
                try:
                    if hasattr(rel, 'setup_related') and rel.rel_model is model:
                        rel.setup_related()
                except ModelNotRegistered:
                    pass
        self._do_prepare_model(model)
        class_prepared.send(sender=model, using=self._using)

    def _inherit(self, successor, parents):
        for base in parents:  # recursive
            if not base.__dict__.get('polymorphic'):
                for name, field in base.declared_fields.items():
                    if name not in successor.declared_fields:
                        successor.declared_fields[name] = field

    def using(self, alias=False):
        if alias is False:
            return self._using
        if alias is None or alias == self._using:
            return self
        c = copy.copy(self)
        c._using = alias
        c.query = c.query.using(c._using)
        c.base_query = c.base_query.using(c._using)
        return c

    @property
    def relations(self):  # bound_relations(), local_relations() ???
        result = {}
        for name in dir(self.model):
            attr = getattr(self.model, name, None)
            if isinstance(attr, RelationDescriptor):
                result[name] = attr.get_bound_relation(self.model)
        return result

    def load(self, data, from_db=True):
        if from_db:
            cols = self.columns
            data_mapped = {}
            for key, value in data:
                try:
                    data_mapped[cols[key].name] = value
                except KeyError:
                    data_mapped[key] = value
        else:
            data_mapped = dict(data)
        identity_map = IdentityMap(self._using)
        key = self._make_identity_key(self.model, tuple(data_mapped[i] for i in to_tuple(self.pk)))
        if identity_map.exists(key):
            try:
                return identity_map.get(key)
            except ObjectDoesNotExist:
                pass
        obj = self._do_load(data_mapped)
        self.set_original_data(obj, data_mapped)
        self.mark_new(obj, False)
        identity_map.add(key, obj)
        return obj

    def _do_load(self, data):
        return self.model(**data)

    def unload(self, obj, fields=frozenset(), exclude=frozenset(), to_db=True):
        if not fields:
            fields = self.fields
        fields = set(fields)  # Can be any iterable type: tuple, list etc.
        fields -= set(exclude)
        data = self._do_unload(obj, fields)
        if to_db:
            # check field is not virtual like annotation or subquery.
            data = {self.fields[name].column: value for name, value in data.items()
                    if not getattr(self.fields[name], 'virtual', False)}
        return data

    def _do_unload(self, obj, fields):
        return {name: getattr(obj, name, None) for name in fields}

    def _make_identity_key(self, model, pk):
        return (model, to_tuple(pk))

    def set_original_data(self, obj, data):
        # TODO: use WeakKeyDictionary?
        obj._original_data = data

    def update_original_data(self, obj, **data):
        self.get_original_data(obj).update(data)

    def get_original_data(self, obj):
        if not hasattr(obj, '_original_data'):
            obj._original_data = {}
        return obj._original_data

    def mark_new(self, obj, status=True):
        if status is not None:
            obj._new_record = status

    def is_new(self, obj, status=None):
        # TODO: use WeakKeyDictionary?
        try:
            return obj._new_record
        except AttributeError:
            obj._new_record = True
            return obj._new_record

    def get_changed(self, obj):
        if not self.get_original_data(obj):
            return set(self.fields)
        return set(k for k, v in self.get_original_data(obj).items() if getattr(obj, k, None) != v)

    def set_defaults(self, obj):
        for name, field in self.fields.items():
            if not hasattr(field, 'default'):
                continue
            default = field.default
            if getattr(obj, name, None) is None:
                if isinstance(default, collections.Callable):
                    try:
                        default(obj, name)
                    except TypeError:
                        default = default()
                setattr(obj, name, default)
        return obj

    def validate(self, obj, fields=frozenset(), exclude=frozenset()):
        self.set_defaults(obj)
        errors = {}
        for name, field in self.fields.items():
            if name in exclude or (fields and name not in fields):
                continue
            if not hasattr(field, 'validators'):
                continue
            for validator in field.validators:
                assert isinstance(validator, collections.Callable), 'The validator must be callable'
                value = getattr(obj, name)
                try:
                    valid_or_msg = validator(obj, name, value)
                except TypeError:
                    valid_or_msg = validator(value)
                if valid_or_msg is not True:
                    # Don't need message code. To rewrite message simple wrap (or extend) validator.
                    errors.setdefault(name, []).append(
                        valid_or_msg or 'Improper value "{0}" for "{1}"'.format(value, name)
                    )
        if errors:
            raise ValidationError(errors)

    def _insert_query(self, obj):
        auto_pk = not all(to_tuple(self.get_pk(obj)))
        data = self.unload(obj, exclude=(to_tuple(self.pk) if auto_pk else ()), to_db=True)
        return smartsql.Insert(table=self.sql_table, map=data)

    def _update_query(self, obj):
        data = self.unload(obj, fields=self.get_changed(obj), to_db=True)
        return smartsql.Update(table=self.sql_table, map=data, where=(self.sql_table.pk == self.get_pk(obj)))

    def _delete_query(self, obj):
        return smartsql.Delete(table=self.sql_table, where=(self.sql_table.pk == self.get_pk(obj)))

    def save(self, obj):
        """Sets defaults, validates and inserts into or updates database"""
        self.set_defaults(obj)
        self.validate(obj, fields=self.get_changed(obj))
        pre_save.send(sender=self.model, instance=obj, using=self._using)
        is_new = self.is_new(obj)
        result = self._insert(obj) if is_new else self._update(obj)
        post_save.send(sender=self.model, instance=obj, created=is_new, using=self._using)
        self.update_original_data(obj, **self.unload(obj, to_db=False))
        self.mark_new(obj, False)
        return result

    def _insert(self, obj):
        cursor = databases[self._using].execute(self._insert_query(obj))
        if not all(to_tuple(self.get_pk(obj))):
            self.set_pk(obj, self.base_query.result.db.last_insert_id(cursor))
        IdentityMap(self._using).add(self._make_identity_key(self.model, self.get_pk(obj)))

    def _update(self, obj):
        databases[self._using].execute(self._update_query(obj))

    def delete(self, obj, visited=None):
        if visited is None:
            visited = set()
        if self in visited:
            return False
        visited.add(self)

        pre_delete.send(sender=self.model, instance=obj, using=self._using)
        for key, rel in self.relations.items():
            if isinstance(rel, OneToMany):
                for child in getattr(obj, key).iterator():
                    rel.on_delete(obj, child, rel, self._using, visited)
            elif isinstance(rel, OneToOne):
                child = getattr(obj, key)
                rel.on_delete(obj, child, rel, self._using, visited)

        databases[self._using].execute(self._delete_query(obj))
        post_delete.send(sender=self.model, instance=obj, using=self._using)
        IdentityMap(self._using).remove(self._make_identity_key(self.model, self.get_pk(obj)))
        return True

    def get(self, _obj_pk=None, **kwargs):
        if _obj_pk is not None:
            identity_map = IdentityMap(self._using)
            key = self._make_identity_key(self.model, _obj_pk)
            if identity_map.exists(key):
                return identity_map.get(key)
            try:
                obj = self.get(**{k: v for k, v in zip(to_tuple(self.pk), to_tuple(_obj_pk))})
            except ObjectDoesNotExist:
                identity_map.add(key)
            else:
                # obj added to identity_map by loader (self.load())
                return obj

        if kwargs:
            q = self.query
            for k, v in kwargs.items():
                q = q.where(self.sql_table.__getattr__(k) == v)
            return q[0]

    def get_pk(self, obj):
        if type(self.pk) == tuple:
            return tuple(getattr(obj, k, None) for k in self.pk)
        return getattr(obj, self.pk, None)

    def set_pk(self, obj, value):
        for k, v in zip(to_tuple(self.pk), to_tuple(value)):
            setattr(obj, k, v)


class ModelBase(type):
    """Metaclass for Model"""
    mapper_class = Mapper

    def __new__(cls, name, bases, attrs):

        new_cls = type.__new__(cls, name, bases, attrs)

        if name in ('Model', 'NewBase', ):
            return new_cls

        mapper_class = getattr(new_cls, 'Mapper', None) or getattr(new_cls, 'Meta', None)
        bases = []
        if mapper_class is not None:
            bases.append(mapper_class)
        if not isinstance(mapper_class, new_cls.mapper_class):
            bases.append(new_cls.mapper_class)

        NewMapper = type("{}Mapper".format(new_cls.__name__), tuple(bases), {})
        NewMapper(new_cls)
        for k in to_tuple(mapper_registry[new_cls].pk):
            setattr(new_cls, k, None)

        return new_cls


class Model(ModelBase(b"NewBase", (object, ), {})):

    _new_record = True
    _s = None

    def __init__(self, *args, **kwargs):
        mapper = mapper_registry[self.__class__]
        pre_init.send(sender=self.__class__, instance=self, args=args, kwargs=kwargs, using=mapper._using)
        if args:
            self.__dict__.update(zip(mapper.fields.keys(), args))
        if kwargs:
            self.__dict__.update(kwargs)
        post_init.send(sender=self.__class__, instance=self, using=mapper._using)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._get_pk() == other._get_pk()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __dir__(self):
        return dir(super(Model, self)) + list(mapper_registry[self.__class__].fields)

    # Use basic __hash__() based on id(self) to be used in WeakKeyDictionary()

    def _get_pk(self):
        return mapper_registry[self.__class__].get_pk(self)

    def _set_pk(self, value):
        return mapper_registry[self.__class__].set_pk(self, value)

    pk = property(_get_pk, _set_pk)

    def validate(self, fields=frozenset(), exclude=frozenset()):
        return mapper_registry[self.__class__].validate(self, fields=fields, exclude=exclude)

    def save(self, using=None):
        return mapper_registry[self.__class__].using(using).save(self)

    def delete(self, using=None, visited=None):
        return mapper_registry[self.__class__].using(using).delete(self)

    @classproperty
    def _mapper(cls):
        return mapper_registry[cls]

    @classproperty
    def s(cls):
        # TODO: Use Model class descriptor without __set__().
        return mapper_registry[cls].sql_table

    @classproperty
    def q(cls):
        return mapper_registry[cls].query

    @classproperty
    def qs(cls):
        smartsql.warn('Model.qs', 'Model.q')
        return mapper_registry[cls].query

    @classmethod
    def get(cls, _obj_pk=None, **kwargs):
        return mapper_registry[cls].get(_obj_pk, **kwargs)

    def __repr__(self):
        return "<{0}.{1}: {2}>".format(type(self).__module__, type(self).__name__, self.pk)


class CompositeModel(object):
    """Composite model.

    Exaple of usage:
    >>> rows = CompositeModel(Model1, Model2).q...filter(...)
    >>> type(rows[0]):
        CompositeModel
    >>> list(rows[0])
        [<Model1: 1>, <Model2: 2>]
    """
    def __init__(self, *models):
        self.models = models

    # TODO: build me.


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


@cr
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

    def __getattr__(self, name):
        if name[0] == '_':
            raise AttributeError
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
            return smartsql.CompositeExpr(*(self.__getattr__(k) for k in field))

        if field in self._mapper.fields:
            field = self._mapper.fields[field].column
        parts[0] = field
        return super(Table, self).__getattr__(smartsql.LOOKUP_SEP.join(parts))


@cr
class TableAlias(smartsql.TableAlias, Table):
    @property
    def _mapper(self):
        return getattr(self._table, '_mapper', None)  # Can be subquery


def cascade(parent, child, parent_rel, using, visited):
    mapper_registry[child.__class__].using(using).delete(child, visited=visited)


def set_null(parent, child, parent_rel, using, visited):
    setattr(child, parent_rel.rel_field, None)
    mapper_registry[child.__class__].using(using).save(child)


def do_nothing(parent, child, parent_rel, using, visited):
    pass

# TODO: descriptor for FileField? Or custom postgresql data type? See http://www.postgresql.org/docs/8.4/static/sql-createtype.html


class BaseRelation(object):

    @cached_property
    def descriptor_class(self):
        for cls in self.owner.__mro__:
            for name, attr in cls.__dict__.items():
                if attr is self.descriptor():
                    return cls
        raise Exception("Can't find descriptor class for {} in {}.".format(self.owner, self.owner.__mro__))

    @cached_property
    def descriptor_object(self):
        for cls in self.owner.__mro__:
            for name, attr in cls.__dict__.items():
                if attr is self.descriptor():
                    return getattr(cls, name)
        raise Exception("Can't find descriptor object")

    @cached_property
    def polymorphic_class(self):
        result_cls = self.descriptor_class
        mro_reversed = list(reversed(self.owner.__mro__))
        mro_reversed = mro_reversed[mro_reversed.index(result_cls) + 1:]
        for cls in mro_reversed:
            if mapper_registry[cls].__dict__.get('polymorphic'):
                break
            result_cls = cls
        return result_cls

    @cached_property
    def name(self):
        self_id = id(self.descriptor())
        for name in dir(self.owner):
            if id(getattr(self.owner, name, None)) == self_id:
                return name

    @cached_property
    def model(self):
        return self.polymorphic_class

    @cached_property
    def rel_model(self):
        if isinstance(self._rel_model_or_name, string_types):
            name = self._rel_model_or_name
            if name == 'self':
                name = mapper_registry[self.model].name
            return model_registry[name]
        return self._rel_model_or_name

    def bind(self, owner):
        c = copy.copy(self)
        c.owner = owner
        return c


class Relation(BaseRelation):

    def __init__(self, rel_model, rel_field=None, field=None, on_delete=cascade, rel_name=None, rel_query=None, query=None):
        if isinstance(rel_model, Mapper):
            rel_model = rel_model.model
        self._rel_model_or_name = rel_model
        self._rel_field = rel_field and to_tuple(rel_field)
        self._field = field and to_tuple(field)
        self.on_delete = on_delete
        self._rel_name = rel_name
        self._query = query
        self._rel_query = rel_query

    @cached_property
    def rel(self):
        return getattr(self.rel_model, self.rel_name).relation

    @cached_property
    def query(self):
        if isinstance(self._query, collections.Callable):
            return self._query(self)
        else:
            return mapper_registry[self.model].query

    @cached_property
    def rel_query(self):
        if isinstance(self._rel_query, collections.Callable):
            return self._rel_query(self)
        else:
            return mapper_registry[self.rel_model].query

    def get_where(self, rel_obj):
        t = mapper_registry[self.model].sql_table
        return t.__getattr__(self.name) == self.get_rel_value(rel_obj)  # Use CompositeExpr
        return reduce(operator.and_,
                      ((t.__getattr__(f) == getattr(rel_obj, rf, None))
                       for f, rf in zip(self.field, self.rel_field)))

    def get_rel_where(self, obj):
        t = mapper_registry[self.rel_model].sql_table
        return t.__getattr__(self.rel_name) == self.get_value(obj)  # Use CompositeExpr
        return reduce(operator.and_,
                      ((t.__getattr__(rf) == getattr(obj, f, None))
                       for f, rf in zip(self.field, self.rel_field)))

    def get_join_where(self):
        t = mapper_registry[self.model].sql_table
        rt = mapper_registry[self.rel_model].sql_table
        return t.__getattr__(self.name) == rt.__getattr__(self.rel_name)  # Use CompositeExpr
        return reduce(operator.and_,
                      ((t.__getattr__(f) == rt.__getattr__(rf))
                       for f, rf in zip(self.field, self.rel_field)))

    def get_value(self, obj):
        return tuple(getattr(obj, f, None) for f in self.field)

    def get_rel_value(self, rel_obj):
        return tuple(getattr(rel_obj, f, None) for f in self.rel_field)

    def set_value(self, obj, value):
        field = self.field
        if value is None:
            value = (None,) * len(field)
        for f, v in zip(field, to_tuple(value)):
            setattr(obj, f, v)

    def set_rel_value(self, rel_obj, value):
        rel_field = self.rel_field
        if value is None:
            value = (None,) * len(rel_field)
        for f, v in zip(rel_field, to_tuple(value)):
            setattr(rel_obj, f, v)

    def validate_rel_obj(self, rel_obj):
        if not isinstance(rel_obj, self.rel_model):
            raise Exception('Object should be an instance of "{0!r}", not "{1!r}".'.format(
                mapper_registry[self.rel_model], type(rel_obj)
            ))

    def _get_cache(self, instance, key):
        try:
            return instance._cache[key]
        except (AttributeError, KeyError):
            return None

    def _set_cache(self, instance, key, value):
        try:
            instance._cache[key] = value
        except AttributeError:
            instance._cache = {}
            self._set_cache(instance, key, value)


class ForeignKey(Relation):

    @cached_property
    def field(self):
        return self._field or ('{0}_id'.format(self.rel_model.__name__.lower()),)

    @cached_property
    def rel_field(self):
        return self._rel_field or to_tuple(mapper_registry[self.rel_model].pk)

    @cached_property
    def rel_name(self):
        if self._rel_name is None:
            return '{0}_set'.format(self.model.__name__.lower())
        elif isinstance(self._rel_name, collections.Callable):
            return self._rel_name(self)
        else:
            return self._rel_name

    def setup_related(self):
        try:
            rel_model = self.rel_model
        except ModelNotRegistered:
            return

        if self.rel_name in mapper_registry[rel_model].relations:
            return

        setattr(rel_model, self.rel_name, RelationDescriptor(OneToMany(
            self.owner, self.field, self.rel_field,
            on_delete=self.on_delete, rel_name=self.name,
            rel_query=self._query
        )))

    def get(self, instance):
        val = self.get_value(instance)
        if not all(val):
            return None

        cached_obj = self._get_cache(instance, self.name)
        rel_field = self.rel_field
        rel_model = self.rel_model
        if cached_obj is None or self.get_rel_value(cached_obj) != val:
            if self._rel_query is None and rel_field == to_tuple(mapper_registry[rel_model].pk):
                obj = mapper_registry[rel_model].get(val)  # to use IdentityMap
            else:
                obj = self.rel_query.where(self.get_rel_where(instance))[0]
            self._set_cache(instance, self.name, obj)
        return self._get_cache(instance, self.name)

    def set(self, instance, value):
        if is_model_instance(value):
            self.validate_rel_obj(value)
            self._set_cache(instance, self.name, value)
            value = self.get_rel_value(value)
        self.set_value(instance, value)

    def delete(self, instance):
        self._set_cache(instance, self.name, None)
        self.set_value(instance, None)


class OneToOne(ForeignKey):

    def setup_related(self):
        try:
            rel_model = self.rel_model
        except ModelNotRegistered:
            return

        if self.rel_name in mapper_registry[rel_model].relations:
            return

        setattr(rel_model, self.rel_name, RelationDescriptor(OneToOne(
            self.owner, self.field, self.rel_field,
            on_delete=self.on_delete, rel_name=self.name,
            rel_query=self._query
        )))
        # self.on_delete = do_nothing


class OneToMany(Relation):

    # TODO: is it need setup_related() here to construct related FK?

    @cached_property
    def field(self):
        return self._field or to_tuple(mapper_registry[self.model].pk)

    @cached_property
    def rel_field(self):
        return self._rel_field or ('{0}_id'.format(self.model.__name__.lower()),)

    @cached_property
    def rel_name(self):
        return self._rel_name or self.model.__name__.lower()

    def get(self, instance):
        val = self.get_value(instance)
        cached_query = self._get_cache(instance, self.name)
        # Be sure that value of related fields equals to value of field
        if cached_query is not None and cached_query._cache is not None:
            for cached_obj in cached_query._cache:
                if self.get_rel_value(cached_obj) != val:
                    cached_query = None
                    break
        if cached_query is None:
            q = self.rel_query.where(self.get_rel_where(instance))
            self._set_cache(instance, self.name, q)
        return self._get_cache(instance, self.name)

    def set(self, instance, object_list):
        val = self.get_value(instance)
        for cached_obj in object_list:
            if is_model_instance(cached_obj):
                self.validate_rel_obj(cached_obj)
                if self.get_rel_value(cached_obj) != val:
                    return
        self.get(instance)._cache = object_list


class ManyToMany(BaseRelation):

    def __init__(self, rel_model, rel_relation, relation):  # associated_model, associated_relation???
        if isinstance(rel_model, Mapper):
            rel_model = rel_model.model
        self._rel_model_or_name = rel_model
        self._rel_relation = rel_relation
        self._relation = relation


class RelationDescriptor(object):

    def __init__(self, relation):
        relation.descriptor = weakref.ref(self)
        self.relation = relation
        self._bound_caches = {}

    def get_bound_relation(self, owner):
        try:
            return self._bound_caches[owner]
        except KeyError:
            self._bound_caches[owner] = self.relation.bind(owner)
            return self.get_bound_relation(owner)

    def __get__(self, instance, owner):
        if not instance:
            return self
        return self.get_bound_relation(owner).get(instance)

    def __set__(self, instance, value):
        self.get_bound_relation(instance.__class__).set(instance, value)

    def __delete__(self, instance):
        self.get_bound_relation(instance.__class__).delete(instance)
