from __future__ import absolute_import
import re
import copy
import collections
import operator
from functools import reduce
from threading import local
from weakref import WeakValueDictionary

from sqlbuilder import smartsql
from . import signals
from .databases import databases
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


class ModelNotRegistered(Exception):
    pass


class ObjectDoesNotExist(Exception):
    pass


class ModelRegistry(dict):

    def add(self, name, model):
        self[name] = model

    def __getitem__(self, name):
        try:
            return super(ModelRegistry, self).__getitem__(name)
        except KeyError:
            raise ModelNotRegistered("""Model {} is not registered in {}""".format(name, self.keys()))

registry = ModelRegistry()


class IdentityMap(object):

    READ_UNCOMMITTED = 0  # IdentityMap is disabled
    READ_COMMITTED = 1  # IdentityMap is disabled
    REPEATABLE_READ = 2  # Prevent repeated DB-query only for existent objects
    SERIALIZABLE = 3  # Prevent repeated DB-query for both, existent and nonexistent objects

    INFLUENCING_LEVELS = (REPEATABLE_READ, SERIALIZABLE)

    _ctx = local()
    _size = 1000
    _isolation_level = READ_UNCOMMITTED  # Disabled currently

    class Nonexistent(object):
        pass

    def __new__(cls, *args, **kwargs):
        if not hasattr(IdentityMap._ctx, 'singleton'):
            self = IdentityMap._ctx.singleton = super(IdentityMap, cls).__new__(cls, *args, **kwargs)
            self._cache = []
            self._alive = WeakValueDictionary()
        return IdentityMap._ctx.singleton

    def add(self, key, value=None):
        if self._isolation_level not in self.INFLUENCING_LEVELS:
            return
        if value is None:
            if self._isolation_level != self.SERIALIZABLE:
                return
            value = self.Nonexistent()
        self._cache.append(value)
        if len(self._cache) > self._size:
            self._cache.pop(0)
        self._alive[key] = value

    def get(self, key):
        if self._isolation_level not in self.INFLUENCING_LEVELS:
            raise KeyError
        value = self._alive[key]
        self._cache.remove(value)
        self._cache.append(value)
        if value.__class__ == self.Nonexistent:
            if self._isolation_level != self.SERIALIZABLE:
                raise KeyError
            raise ObjectDoesNotExist
        return value

    def remove(self, key):
        value = self._alive[key]
        self._cache.remove(value)
        del self._alive[key]

    def clear(self):
        del self._cache[:]
        self._alive.clear()

    def exists(self, key):
        if self._isolation_level not in self.INFLUENCING_LEVELS:
            return False
        return key in self._alive

    def set_size(self, size):
        self._size = size

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

    _gateway = None
    _raw = None
    _cache = None
    _using = 'default'

    def __init__(self, gateway):
        self._prefetch = {}
        self._select_related = {}
        self.is_base(True)
        self._mapping = default_mapping
        self._gateway = gateway
        self._using = gateway._using

    def __len__(self):
        """Returns length or list."""
        self.fill_cache()
        return len(self._cache)

    def __iter__(self):
        """Returns iterator."""
        self.fill_cache()
        return iter(self._cache)

    def __getitem__(self, key):
        """Returns sliced self or item."""
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
        """Returns length or list."""
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
        return self

    def iterator(self):
        """iterator"""
        cursor = self.execute()
        descr = cursor.description
        fields = tuple(f[0] for f in descr)
        state = {}
        for row in cursor.fetchall():
            yield self._mapping(self, zip(fields, row), state)

    def using(self, alias=False):
        if alias is None:
            return self._using
        if alias is not False:
            self._using = alias
        return self

    def is_base(self, value=None):
        if value is None:
            return self._is_base
        self._is_base = value
        return self

    @property
    def db(self):
        return databases[self._using]

    def map(self, mapping):
        """Sets mapping."""
        c = self
        c._mapping = mapping
        return c

    def prefetch(self, *a, **kw):
        """Prefetch relations"""
        relations = self._gateway.bound_relations
        if a and not a[0]:  # .prefetch(False)
            self._prefetch = {}
        else:
            self._prefetch = copy.copy(self._prefetch)
            self._prefetch.update(kw)
            self._prefetch.update({i: relations[i].query for i in a})
        return self

    def populate_prefetch(self):
        relations = self._gateway.bound_relations
        for key, q in self._prefetch.items():
            rel = relations[key]
            # recursive handle prefetch
            field = rel.field
            rel_field = rel.rel_field

            cond = reduce(operator.or_,
                          (reduce(operator.and_,
                                  ((rel.rel_model.s.__getattr__(rf) == getattr(obj, f))
                                   for f, rf in zip(field, rel_field)))
                           for obj in self._cache))
            rows = list(q.where(cond))
            for obj in self._cache:
                val = [i for i in rows if tuple(getattr(i, f) for f in rel_field) == tuple(getattr(obj, f) for f in field)]
                if isinstance(rel.relation, (ForeignKey, OneToOne)):
                    val = val[0] if val else None
                    if val and isinstance(rel.relation, OneToOne):
                        setattr(val, rel.rel_name, obj)
                elif isinstance(rel.relation, OneToMany):
                    for i in val:
                        setattr(i, rel.rel_name, obj)
                setattr(obj, key, val)


class Gateway(object):
    """Gateway"""

    pk = 'id'
    default_using = 'default'
    _using = 'default'
    abstract = False
    field_factory = Field
    result_factory = Result

    def __init__(self, model=None):
        """Instance constructor"""
        if model:
            self.model = model

        if not hasattr(self, 'name'):
            self.name = self._create_default_name(model)

        registry.add(self.name, model)
        self.declared_fields = self._create_declared_fields(
            model,
            getattr(self, 'map', {}),
            getattr(self, 'defaults', {}),
            getattr(self, 'validations', {}),
            getattr(self, 'declared_fields', {})
        )

        self._prepare_model(model)
        self._inherit(self, (base._gateway for base in self.model.__bases__ if hasattr(base, '_gateway')))  # recursive

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
        field._gateway = self
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
        model._gateway = model._meta = self
        for name in self.model.__dict__:
            field = getattr(model, name, None)
            if isinstance(field, Field):
                delattr(model, name)

        # TODO: use dir() instead __dict__ to handle relations in abstract classes,
        # add templates support for related_name,
        # support copy.
        for key, rel in model.__dict__.items():
            if isinstance(rel, Relation) and hasattr(rel, 'add_related'):
                try:
                    rel.add_related(model)
                except ModelNotRegistered:
                    pass

        for rel_model in registry.values():
            for key, rel in rel_model._gateway.relations.items():
                try:
                    if hasattr(rel, 'add_related') and rel.rel_model(rel_model) is model:
                        rel.add_related(rel_model)
                except ModelNotRegistered:
                    pass
        self._do_prepare_model(model)
        signals.send_signal(signal='class_prepared', sender=model, using=self._using)

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
    def relations(self):
        result = {}
        for name in dir(self.model):
            attr = getattr(self.model, name, None)
            if isinstance(attr, Relation):
                result[name] = attr
        return result

    @property
    def bound_relations(self):  # local_relations() ???
        result = {}
        for name in dir(self.model):
            attr = getattr(self.model, name, None)
            if isinstance(attr, Relation):
                result[name] = attr
        return {k: BoundRelation(self.model, v) for k, v in self.relations.items()}

    def load_object(self, data, from_db=True):
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
        identity_map = IdentityMap()
        key = (self.model, tuple(data_mapped[i] for i in to_tuple(self.pk)))
        if identity_map.exists(key):
            try:
                return identity_map.get(key)
            except ObjectDoesNotExist:
                pass
        obj = self.model(**data_mapped)
        obj._original_data = data_mapped
        obj._new_record = False
        identity_map.add(key, obj)
        return obj

    def get_data(self, obj, fields=frozenset(), exclude=frozenset(), to_db=True):
        data = {name: getattr(obj, name, None)
                for name in self.fields
                if not (name in exclude or (fields and name not in fields))}
        if to_db:
            # check field is not virtual like annotation and subquery.
            data = {self.fields[name].column: value for name, value in data.items() if not getattr(self.fields[name], 'virtual', False)}
        return data

    def get_changed(self, obj):
        if not hasattr(obj, '_original_data'):
            return set(self.fields)
        return set(k for k, v in obj._original_data.items() if getattr(obj, k, None) != v)

    def send_signal(self, sender, *a, **kw):
        """Sends signal"""
        kw.update({'sender': self.model, 'instance': sender})
        return signals.send_signal(*a, **kw)

    def set_defaults(self, obj):
        """Sets attribute defaults."""
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
        """Tests all ``validations``"""
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

    def save(self, obj):
        """Sets defaults, validates and inserts into or updates database"""
        self.set_defaults(obj)
        self.validate(obj, fields=self.get_changed(obj))
        self.send_signal(obj, signal='pre_save', using=self._using)
        result = self._insert(obj) if obj._new_record else self._update(obj)
        self.send_signal(obj, signal='post_save', created=obj._new_record, using=self._using)
        if not hasattr(obj, '_original_data'):
            obj._original_data = {}
        obj._original_data.update(self.get_data(obj, to_db=False))
        obj._new_record = False
        return result

    def _insert(self, obj):
        query = self.base_query
        pk = to_tuple(self.pk)
        auto_pk = not all(getattr(obj, k, False) for k in pk)
        data = self.get_data(obj, exclude=(pk if auto_pk else ()), to_db=True)
        cursor = query.insert(data)
        if auto_pk:
            for k, v in zip(pk, to_tuple(query.result.db.last_insert_id(cursor))):
                setattr(obj, k, v)

    def _update(self, obj):
        pk = to_tuple(self.pk)
        cond = reduce(operator.and_, (smartsql.Field(self.fields[k].column, self.sql_table) == getattr(obj, k) for k in pk))
        data = self.get_data(obj, fields=self.get_changed(obj), to_db=True)
        self.base_query.where(cond).update(data)

    def delete(self, obj, visited=None):
        """Deletes record from database"""

        if visited is None:
            visited = set()
        if self in visited:
            return False
        visited.add(self)

        self.send_signal(obj, signal='pre_delete', using=self._using)
        for key, rel in self.relations.items():
            if isinstance(rel, OneToMany):
                for child in getattr(obj, key).iterator():
                    rel.on_delete(obj, child, rel, self._using, visited)
            elif isinstance(rel, OneToOne):
                child = getattr(obj, key)
                rel.on_delete(obj, child, rel, self._using, visited)

        pk = to_tuple(self.pk)
        cond = reduce(operator.and_, (smartsql.Field(self.fields[k].column, self.sql_table) == getattr(obj, k) for k in pk))
        self.base_query.where(cond).delete()
        self.send_signal(obj, signal='post_delete', using=self._using)
        return True

    def get(self, _obj_pk=None, **kwargs):
        """Returns Q object"""
        if _obj_pk is not None:
            identity_map = IdentityMap()
            key = (self.model, tuple(to_tuple(_obj_pk)))
            if identity_map.exists(key):
                return identity_map.get(key)
            try:
                obj = self.get(**{k: v for k, v in zip(to_tuple(self.pk), to_tuple(_obj_pk))})
            except ObjectDoesNotExist:
                identity_map.add(key)
            else:
                # obj added to identity_map by loader (self.load_object())
                return obj

        if kwargs:
            q = self.query
            for k, v in kwargs.items():
                q = q.where(smartsql.Field(self.fields[k].column, self.sql_table) == v)
            return q[0]


class ModelBase(type):
    """Metaclass for Model"""
    gateway_class = Gateway

    def __new__(cls, name, bases, attrs):

        new_cls = type.__new__(cls, name, bases, attrs)

        if name in ('Model', 'NewBase', ):
            return new_cls

        if hasattr(new_cls, 'Gateway'):
            if isinstance(new_cls.Gateway, new_cls.gateway_class):
                NewGateway = new_cls.Gateway
            else:
                class NewGateway(new_cls.Gateway, new_cls.gateway_class):
                    pass
        elif hasattr(new_cls, 'Meta'):  # backward compatible
            if isinstance(new_cls.Meta, new_cls.gateway_class):
                NewGateway = new_cls.Meta
            else:
                class NewGateway(new_cls.Meta, new_cls.gateway_class):
                    pass
        else:
            NewGateway = new_cls.gateway_class
        NewGateway(new_cls)
        for k in to_tuple(new_cls._gateway.pk):
            setattr(new_cls, k, None)

        return new_cls


class Model(ModelBase(b"NewBase", (object, ), {})):
    """Model class"""

    _new_record = True
    _s = None

    def __init__(self, *args, **kwargs):
        """Allows setting of fields using kwargs"""
        gateway = self._gateway
        signals.send_signal(signal='pre_init', sender=self.__class__, instance=self, args=args, kwargs=kwargs, using=gateway._using)
        if args:
            self.__dict__.update(zip(gateway.fields.keys(), args))
        if kwargs:
            self.__dict__.update(kwargs)
        signals.send_signal(signal='post_init', sender=self.__class__, instance=self, using=gateway._using)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._get_pk() == other._get_pk()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._get_pk())

    def _get_pk(self):
        """Sets the current value of the primary key"""
        pk = self._gateway.pk
        if type(pk) == tuple:
            return tuple(getattr(self, k, None) for k in pk)
        return getattr(self, pk, None)

    def _set_pk(self, value):
        """Sets the primary key"""
        for k, v in zip(to_tuple(self._gateway.pk), to_tuple(value)):
            setattr(self, k, v)

    pk = property(_get_pk, _set_pk)

    def validate(self, fields=frozenset(), exclude=frozenset()):
        return self._gateway.validate(self, fields=fields, exclude=exclude)

    def save(self, using=None):
        return self._gateway.using(using).save(self)

    def delete(self, using=None, visited=None):
        return self._gateway.using(using).delete(self)

    @classproperty
    def s(cls):
        # TODO: Use Model class descriptor without __set__().
        return cls._gateway.sql_table

    @classproperty
    def q(cls):
        return cls._gateway.query

    @classproperty
    def qs(cls):
        smartsql.warn('Model.qs', 'Model.q')
        return cls._gateway.query

    @classmethod
    def get(cls, _obj_pk=None, **kwargs):
        return cls._gateway.get(_obj_pk, **kwargs)

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
    return result._gateway.load_object(row, from_db=True)


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
            pk = to_tuple(model._gateway.pk)
            pk_columns = tuple(model._gateway.fields[k].columns for k in pk)
            model_row_dict = dict(model_row)
            pk_values = tuple(model_row_dict[k] for k in pk_columns)
            key = (model, pk_values)
            if key not in state:
                state[key] = model._gateway.load_object(model_row, from_db=True)
            objs.append(state[key])
        return objs

    def build_relations(self, relations, objs):
        for i, rel in enumerate(relations):
            obj, rel_obj = objs[i], objs[i + 1]
            name = rel.name
            rel_name = rel.rel_name
            if isinstance(rel.relation, (ForeignKey, OneToOne)):
                setattr(obj, name, rel_obj)
                if not hasattr(rel_obj, rel_name):
                    setattr(rel_obj, rel_name, [])
                getattr(rel_obj, rel_name).append[obj]
            elif isinstance(rel.relation, OneToMany):
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
    """Table class"""

    def __init__(self, gateway, *args, **kwargs):
        """Constructor"""
        super(Table, self).__init__(gateway.db_table, *args, **kwargs)
        self._gateway = gateway

    @property
    def q(self):
        return self._gateway.query

    @property
    def qs(self):
        smartsql.warn('Table.qs', 'Table.q')
        return self._gateway.query

    def get_fields(self, prefix=None):
        """Returns field list."""
        return self._gateway.get_sql_fields()

    def __getattr__(self, name):
        """Added some specific functional."""
        if name[0] == '_':
            raise AttributeError
        parts = name.split(smartsql.LOOKUP_SEP, 1)
        field = parts[0]
        # result = {'field': field, }
        # signals.send_signal(signal='field_conversion', sender=self, result=result, field=field, model=self.model)
        # field = result['field']

        if field == 'pk':
            field = self._gateway.pk
        elif isinstance(self._gateway.relations.get(field, None), ForeignKey):
            field = self._gateway.relations.get(field).field(self._gateway.model)

        if type(field) == tuple:
            if len(parts) > 1:
                raise Exception("Can't set single alias for multiple fields of composite key {}.{}".format(self.model, name))
            return smartsql.CompositeExpr(*(self.__getattr__(k) for k in field))

        if field in self._gateway.fields:
            field = self._gateway.fields[field].column
        parts[0] = field
        return super(Table, self).__getattr__(smartsql.LOOKUP_SEP.join(parts))


def cascade(parent, child, parent_rel, using, visited):
    child._gateway.using(using).delete(child, visited=visited)


def set_null(parent, child, parent_rel, using, visited):
    setattr(child, parent_rel.rel_field, None)
    child._gateway.using(using).save(child)


def do_nothing(parent, child, parent_rel, using, visited):
    pass

# TODO: descriptor for FileField? Or custom postgresql data type? See http://www.postgresql.org/docs/8.4/static/sql-createtype.html


class BoundRelation(object):

    def __init__(self, owner, relation):
        self._owner = owner
        self._relation = relation

    @cached_property
    def relation(self):
        return self._relation

    @cached_property
    def descriptor_class(self):
        return self._relation.descriptor_class(self._owner)

    @cached_property
    def descriptor_object(self):
        return self._relation.descriptor_object(self._owner)

    @cached_property
    def model(self):
        return self._relation.model(self._owner)

    @cached_property
    def query(self):
        return self._relation.query(self._owner)

    @cached_property
    def name(self):
        return self._relation.name(self._owner)

    @cached_property
    def field(self):
        return self._relation.field(self._owner)

    @cached_property
    def rel(self):
        return self._relation.rel(self._owner)

    @cached_property
    def rel_name(self):
        return self._relation.rel_name(self._owner)

    @cached_property
    def rel_model(self):
        return self._relation.rel_model(self._owner)

    @cached_property
    def rel_field(self):
        return self._relation.rel_field(self._owner)

    def __get__(self, instance, owner):
        return self._relation.__get__(instance, self._owner)

    def __set__(self, instance, value):
        return self._relation.__get__(instance, value)

    def __delete__(self, instance):
        return self._relation.__delete__(instance)


class Relation(object):

    def __init__(self, rel_model, rel_field=None, field=None, on_delete=cascade, rel_name=None, query=None, rel_query=None):
        self.rel_model_or_name = rel_model
        self._rel_field = rel_field and to_tuple(rel_field)
        self._field = field and to_tuple(field)
        self.on_delete = on_delete
        self._rel_name = rel_name
        self._query = query
        self._rel_query = rel_query

    def descriptor_class(self, owner):
        for cls in owner.__mro__:
            for name, attr in cls.__dict__.items():
                if attr is self:
                    return cls
        raise Exception("Can't find descriptor class for {} in {}.".format(owner, owner.__mro__))

    def descriptor_object(self, owner):
        for cls in owner.__mro__:
            for name, attr in cls.__dict__.items():
                if attr is self:
                    return getattr(cls, name)
        raise Exception("Can't find descriptor object")

    def polymorphic_class(self, owner):
        result_cls = self.descriptor_class(owner)
        mro_reversed = list(reversed(owner.__mro__))
        mro_reversed = mro_reversed[mro_reversed.index(result_cls) + 1:]
        for cls in mro_reversed:
            if cls._gateway.__dict__.get('polymorphic'):
                break
            result_cls = cls
        return result_cls

    def name(self, owner):
        self_id = id(self)
        for name in dir(owner):
            if id(getattr(owner, name, None)) == self_id:
                return name

    def model(self, owner):
        return self.polymorphic_class(owner)

    def rel(self, owner):
        return getattr(self.rel_model(owner), self.rel_name(owner))

    def rel_model(self, owner):
        if isinstance(self.rel_model_or_name, string_types):
            name = self.rel_model_or_name
            if name == 'self':
                name = self.model(owner)._gateway.name
            return registry[name]
        return self.rel_model_or_name

    def query(self, owner):
        if isinstance(self._query, collections.Callable):
            return self._query(self, owner)
        else:
            return self.rel_model(owner)._gateway.query

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

    def field(self, owner):
        return self._field or ('{0}_id'.format(self.rel_model(owner).__name__.lower()),)

    def rel_field(self, owner):
        return self._rel_field or to_tuple(self.rel_model(owner)._gateway.pk)

    def rel_name(self, owner):
        if self._rel_name is None:
            return '{0}_set'.format(self.model(owner).__name__.lower())
        elif isinstance(self._rel_name, collections.Callable):
            return self._rel_name(self, owner)
        else:
            return self._rel_name

    def add_related(self, owner):
        try:
            rel_model = self.rel_model(owner)
        except ModelNotRegistered:
            return

        if self.rel_name(owner) in rel_model._gateway.relations:
            return

        setattr(rel_model, self.rel_name(owner), OneToMany(
            owner, self.field(owner), self.rel_field(owner),
            on_delete=self.on_delete, rel_name=self.name(owner),
            query=self._rel_query
        ))

    def __get__(self, instance, owner):
        if not instance:
            return self
        val = tuple(getattr(instance, f, None) for f in self.field(owner))
        if not all(val):
            return None

        cached_obj = self._get_cache(instance, self.name(owner))
        rel_field = self.rel_field(owner)
        rel_model = self.rel_model(owner)
        if cached_obj is None or tuple(getattr(cached_obj, f, None) for f in rel_field) != val:
            if self._query is None and rel_field == to_tuple(rel_model._gateway.pk):
                obj = rel_model.get(val)  # to use IdentityMap
            else:
                t = rel_model._gateway.sql_table
                q = self.query(owner)
                for f, v in zip(rel_field, val):
                    q = q.where(t.__getattr__(f) == v)
                obj = q[0]
            self._set_cache(instance, self.name(owner), obj)
        return self._get_cache(instance, self.name(owner))

    def __set__(self, instance, value):
        owner = instance.__class__
        if isinstance(value, Model):
            if not isinstance(value, self.rel_model(owner)):
                raise Exception(
                    'Value should be an instance of "{0}" or primary key of related instance.'.format(
                        self.rel_model(owner)._gateway.name
                    )
                )
            self._set_cache(instance, self.name(owner), value)
            value = tuple(getattr(value, f) for f in self.rel_field(owner))
        value = to_tuple(value)
        for a, v in zip(self.field(owner), value):
            setattr(instance, a, v)

    def __delete__(self, instance):
        owner = instance.__class__
        self._set_cache(instance, self.name(owner), None)
        for a in self.field(owner):
            setattr(instance, a, None)


class OneToOne(ForeignKey):

    def add_related(self, owner):
        try:
            rel_model = self.rel_model(owner)
        except ModelNotRegistered:
            return

        if self.rel_name(owner) in rel_model._gateway.relations:
            return

        setattr(rel_model, self.rel_name(owner), OneToOne(
            owner, self.field(owner), self.rel_field(owner),
            on_delete=self.on_delete, rel_name=self.name(owner),
            query=self._rel_query
        ))
        # self.on_delete = do_nothing


class OneToMany(Relation):

    # TODO: is it need add_related() here to construct related FK?

    def field(self, owner):
        return self._field or to_tuple(self.model(owner)._gateway.pk)

    def rel_field(self, owner):
        return self._rel_field or ('{0}_id'.format(self.model(owner).__name__.lower()),)

    def rel_name(self, owner):
        return self._rel_name or self.model(owner).__name__.lower()

    def __get__(self, instance, owner):
        if not instance:
            return self
        rel_field = self.rel_field(owner)
        val = tuple(getattr(instance, f) for f in self.field(owner))
        cached_query = self._get_cache(instance, self.name(owner))
        # TODO: Be sure that value of related fields equals to value of field
        if cached_query is not None and cached_query._cache is not None:
            for cached_obj in cached_query._cache:
                if tuple(getattr(cached_obj, f, None) for f in rel_field) != val:
                    cached_query = None
                    break
        if cached_query is None:
            t = self.rel_model(owner)._gateway.sql_table
            q = self.query(owner)
            for f, v in zip(self.rel_field(owner), val):
                q = q.where(t.__getattr__(f) == v)
            self._set_cache(instance, self.name(owner), q)
        return self._get_cache(instance, self.name(owner))

    def __set__(self, instance, object_list):
        owner = instance.__class__
        rel_field = self.rel_field(owner)
        val = tuple(getattr(instance, f) for f in self.field(owner))
        for cached_obj in object_list:
            if isinstance(cached_obj, Model):
                if not isinstance(cached_obj, self.rel_model(owner)):
                    raise Exception(
                        'Value should be an instance of "{0}" or primary key of related instance.'.format(
                            self.rel_model(owner)._gateway.name
                        )
                    )
                if tuple(getattr(cached_obj, f, None) for f in rel_field) != val:
                    return
        self.__get__(instance, owner)._cache = object_list
