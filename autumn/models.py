from __future__ import absolute_import
import re
import copy
import collections
import operator
from functools import reduce
from sqlbuilder import smartsql
from . import signals
from .connections import get_db
from .utils import classproperty
from .validators import ValidationError

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)

cr = copy.copy(smartsql.cr)


class ModelNotRegistered(Exception):
    pass


class ModelRegistry(dict):

    def add(self, name, model):
        self[name] = model

    def __getitem__(self, name):
        try:
            return self[name]
        except KeyError:
            raise ModelNotRegistered

registry = ModelRegistry()


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
            return list(self)[0]
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
        return get_db(self._using)

    def map(self, mapping):
        """Sets mapping."""
        c = self
        c._mapping = mapping
        return c

    def prefetch(self, *a, **kw):
        """Prefetch relations"""
        if a and not a[0]:  # .prefetch(False)
            self._prefetch = {}
        else:
            self._prefetch = copy.copy(self._prefetch)
            self._prefetch.update(kw)
            self._prefetch.update({i: self._gateway.relations[i].rel_model(self._gateway.model)._gateway.base_query for i in a})
        return self

    def populate_prefetch(self):
        for key, qs in self._prefetch.items():
            owner = self._gateway.model
            rel = self._gateway.relations[key]
            # recursive handle prefetch
            field = rel.field(owner) if type(rel.field(owner)) == tuple else (rel.field(owner),)
            rel_field = rel.rel_field(owner) if type(rel.rel_field(owner)) == tuple else (rel.rel_field(owner),)

            cond = reduce(operator.or_,
                          (reduce(operator.and_,
                                  ((rel.rel_model(owner).s.__getattr__(rf) == getattr(obj, f))
                                   for f, rf in zip(field, rel_field)))
                           for obj in self._cache))
            rows = list(qs.where(cond))
            for obj in self._cache:
                val = [i for i in rows if tuple(getattr(i, f) for f in rel_field) == tuple(getattr(obj, f) for f in field)]
                if isinstance(rel, (ForeignKey, OneToOne)):
                    val = val[0] if val else None
                    if val and isinstance(rel, OneToOne):
                        setattr(val, "{}_prefetch".format(rel.rel_name(owner)), obj)
                elif isinstance(rel, OneToMany):
                    for i in val:
                        setattr(i, "{}_prefetch".format(rel.rel_name(owner)), obj)
                setattr(obj, "{}_prefetch".format(key), val)


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
        self._inherit(self, (base for base in self.model.__bases__ if hasattr(base, '_gateway')))  # recursive

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

    def _read_columns(self, db_table, using):
        db = get_db(using)
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
        return smartsql.QS(self.sql_table, result=self.result_factory(self)).fields(self.get_sql_fields())

    def _create_query(self):
        """For selection."""
        return smartsql.QS(self.sql_table, result=self.result_factory(self)).fields(self.get_sql_fields())

    def get_sql_fields(self, prefix=None):
        """Returns field list."""
        return [smartsql.Field(f.column, prefix if prefix is not None else self.sql_table) for f in self.fields.values()]

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
            if isinstance(rel, Relation):
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

    def _inherit(self, successor, parents):
        for base in parents:  # recursive
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
    def bound_relations(self):
        result = {}
        for name in dir(self.model):
            attr = getattr(self.model, name, None)
            if isinstance(attr, Relation):
                result[name] = attr
        return {k: BoundRelation(self.model, v) for k, v in self.relations.items()}

    def create_instance(self, data):
        data = dict(data)
        obj = self.model(**data)
        obj._original_data = data
        obj._new_record = False
        return obj

    def get_data(self, obj, fields=frozenset(), exclude=frozenset()):
        data = tuple((name, getattr(obj, name, None))
                     for name in self.fields
                     if not (name in exclude or (fields and name not in fields)))
        return data

    def get_changed(self, obj):
        if not hasattr(obj, '_original_data'):
            return set(self.fields)
        return set(k for k, v in obj._original_data.items() if getattr(obj, k, None) != v)

    def send_signal(self, sender, *a, **kw):
        """Sends signal"""
        kw.update({'sender': type(sender), 'instance': sender})
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
        obj._original_data = self.get_data(obj)
        obj._new_record = False
        return result

    def _insert(self, obj):
        query = self.query
        data = dict(self.get_data(obj))
        pk = self.pk if type(self.pk) == tuple else (self.pk,)
        auto_pk = not all(data.get(k) for k in pk)
        data = {self.fields[name].column: value for name, value in data.items() if not auto_pk or name not in pk}
        cursor = query.insert(data)
        if auto_pk:
            obj.pk = query.result.db.last_insert_id(cursor)

    def _update(self, obj):
        pk = self.pk if type(self.pk) == tuple else (self.pk,)
        cond = reduce(operator.and_, (smartsql.Field(self.fields[k].column, self.sql_table) == getattr(obj, k) for k in pk))
        data = self.get_data(obj, fields=self.get_changed(obj))
        data = {self.fields[name].column: value for name, value in data}
        self.query.where(cond).update(data)

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

        pk = self.pk if type(self.pk) == tuple else (self.pk,)
        cond = reduce(operator.and_, (smartsql.Field(self.fields[k].column, self.sql_table) == getattr(obj, k) for k in pk))
        self.query.where(cond).delete()
        self.send_signal(obj, signal='post_delete', using=self._using)
        return True

    def get(self, _obj_pk=None, **kwargs):
        """Returns QS object"""
        if _obj_pk is not None:
            pk = self.pk
            if type(self.pk) != tuple:
                pk = (self.pk,)
                _obj_pk = (_obj_pk,)
            return self.get(**{k: v for k, v in zip(pk, _obj_pk)})

        if kwargs:
            qs = self.query
            for k, v in kwargs.items():
                qs = qs.where(smartsql.Field(self.fields[k].column, self.sql_table) == v)
            return qs[0]


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

        signals.send_signal(signal='class_prepared', sender=new_cls, using=new_cls._gateway._using)
        return new_cls


class Model(ModelBase(b"NewBase", (object, ), {})):
    """Model class"""

    _new_record = True
    _s = None

    def __init__(self, *args, **kwargs):
        """Allows setting of fields using kwargs"""
        self._gateway.send_signal(self, signal='pre_init', args=args, kwargs=kwargs, using=self._gateway._using)
        self._cache = {}
        pk = self._gateway.pk
        if type(pk) == tuple:
            for k in pk:
                self.__dict__[k] = None
        else:
            self.__dict__[pk] = None
        if args:
            for i, arg in enumerate(args):
                setattr(self, self._gateway.fields.keys()[i], arg)
        if kwargs:
            for k, v in kwargs.items():
                setattr(self, k, v)
        self._gateway.send_signal(self, signal='post_init', using=self._gateway._using)

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
        pk = self._gateway.pk
        if type(pk) == tuple:
            for k, v in zip(pk, value):
                setattr(self, k, v)
        else:
            setattr(self, self._gateway.pk, value)

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
    def qs(cls):
        return cls._gateway.query

    @classmethod
    def get(cls, _obj_pk=None, **kwargs):
        return cls._gateway.get(_obj_pk, **kwargs)

    def __repr__(self):
        return "<{0}.{1}: {2}>".format(type(self).__module__, type(self).__name__, self.pk)


class CompositeModel(object):
    """Composite model.

    Exaple of usage:
    >>> rows = CompositeModel(Model1, Model2).qs...filter(...)
    >>> type(rows[0]):
        CompositeModel
    >>> list(rows[0])
        [<Model1: 1>, <Model2: 2>]
    """
    def __init__(self, *models):
        self.models = models

    # TODO: build me.


def suffix_mapping(result, row, state):
    data = {}
    for k, v in row:
        fn = k
        if fn in data:
            c = 2
            fn_base = fn
            while fn in data:
                fn = fn_base + c
                c += 1
        data[fn] = v
    return default_mapping(result, data.items(), state)


def default_mapping(result, row, state):
    columns = result._gateway.columns
    row = tuple((columns[key].name if key in columns else key, value)
                for key, value in row)
    try:
        return result._gateway.create_instance(row)
    except AttributeError:
        dict(row)


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
            columns = model._gateway.columns
            model_row = tuple((columns[key].name if key in columns else key, value)
                              for key, value in model_row)
            model_row_dict = dict(model_row)
            pk = model._gateway.pk
            if type(pk) != tuple:
                pk = (pk,)
            key = (model, tuple(model_row_dict[f] for f in pk))
            if key not in state:
                state[key] = model._gateway.create_instance(model_row)
            objs.append(state[key])
        return objs

    def build_relations(self, relations, objs):
        for i, rel in enumerate(relations):
            obj, rel_obj = objs[i], objs[i + 1]
            name = '{}_related'.format(rel.name)
            rel_name = '{}_related'.format(rel.rel_name)
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
    """Table class"""

    def __init__(self, gateway, qs=None, *args, **kwargs):
        """Constructor"""
        super(Table, self).__init__(gateway.db_table, *args, **kwargs)
        self._gateway = gateway

    @property
    def qs(self):
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


class cached_property(object):
    def __init__(self, func, name=None):
        self.func = func
        self.name = name or func.__name__

    def __get__(self, instance, type=None):
        if instance is None:
            return self
        res = instance.__dict__[self.name] = self.func(instance)
        return res


class BoundRelation(object):

    def __init__(self, owner, relation):
        self._owner = owner
        self._relation = relation

    @cached_property
    def model(self):
        return self._owner

    @cached_property
    def name(self):
        return self._relation.name(self._owner)

    @cached_property
    def field(self):
        return self._relation.field(self._owner)

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

    def __init__(self, rel_model, rel_field=None, field=None, on_delete=cascade, rel_name=None):
        self.rel_model_or_name = rel_model
        self._rel_field = rel_field
        self._field = field
        self.on_delete = on_delete
        self._rel_name = rel_name

    def name(self, owner):
        self_id = id(self)
        for name in dir(owner):
            if id(getattr(owner, name, None)) == self_id:
                return name

    def rel_model(self, owner):
        if isinstance(self.rel_model_or_name, string_types):
            name = self.rel_model_or_name
            if name == 'self':
                name = self.model._gateway.name
            return registry[name]
        return self.rel_model_or_name


class ForeignKey(Relation):

    def field(self, owner):
        return self._field or '{0}_id'.format(self.rel_model(owner)._gateway.db_table.rsplit("_", 1).pop())

    def rel_field(self, owner):
        return self._rel_field or self.rel_model(owner)._gateway.pk

    def rel_name(self, owner):
        return self._rel_name or '{0}_set'.format(self.rel_model(owner).__name__.lower())

    def add_related(self, owner):
        try:
            rel_model = self.rel_model(owner)
        except ModelNotRegistered:
            return

        if self.rel_name(owner) in rel_model._gateway.relations:
            return

        setattr(rel_model, self.rel_name(owner), OneToMany(
            owner, self.field(owner), self.rel_field(owner),
            on_delete=self.on_delete, rel_name=self.name(owner)
        ))

    def __get__(self, instance, owner):
        # TODO: owner is self.model. self.model is useless (do remove it). It cause problems with inheritance.
        if not instance:
            return self
        field = self.field(owner) if type(self.field(owner)) == tuple else (self.field(owner),)
        rel_field = self.rel_field(owner) if type(self.rel_field(owner)) == tuple else (self.rel_field(owner),)
        fk_val = tuple(getattr(instance, f) for f in field)
        if not [i for i in fk_val if i is not None]:
            return None

        if (getattr(instance._cache.get(self.name(owner), None), f, None) for f in self.rel_field(owner)) != fk_val:
            t = self.rel_model(owner)._gateway.sql_table
            q = self.rel_model(owner)._gateway.base_query
            for f, v in zip(rel_field, fk_val):
                q = q.where(t.__getattr__(f) == v)
            # TODO: Add hook here?
            instance._cache[self.name(owner)] = q[0]
        return instance._cache[self.name(owner)]

    def __set__(self, instance, value):
        owner = instance.__class__
        if isinstance(value, Model):
            if not isinstance(value, self.rel_model(owner)):
                raise Exception(
                    ('Value should be an instance of "{0}" ' +
                     'or primary key of related instance.').format(
                        self.rel_model(owner)._gateway.name
                    )
                )
            instance._cache[self.name(owner)] = value
            value = value._get_pk()
        if type(self.field(owner)) == tuple:
            for a, v in zip(self.field(owner), value):
                setattr(instance, a, v)
        else:
            setattr(instance, self.field(owner), value)

    def __delete__(self, instance):
        owner = instance.__class__
        instance._cache.pop(self.name(owner), None)
        if type(self.field(owner)) == tuple:
            for a in self.field(owner):
                setattr(instance, a, None)
        else:
            setattr(instance, self.field(owner), None)


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
            on_delete=self.on_delete, rel_name=self.name(owner)
        ))
        self.on_delete = do_nothing


class OneToMany(Relation):

    # TODO: is it need add_related() here to construct related FK?

    def field(self, owner):
        return self._field or owner._gateway.pk

    def rel_field(self, owner):
        return self._rel_field or '{0}_id'.format(owner._gateway.db_table.rsplit("_", 1).pop())

    def rel_name(self, owner):
        return self._rel_name or self.rel_model(owner).__name__.lower()

    def __get__(self, instance, owner):
        if not instance:
            return self
        field = self.field(owner) if type(self.field(owner)) == tuple else (self.field(owner),)
        rel_field = self.rel_field(owner) if type(self.rel_field(owner)) == tuple else (self.rel_field(owner),)
        val = tuple(getattr(instance, f) for f in field)
        # Cache attr already exists in QS, so, can be even setable.
        t = self.rel_model(owner)._gateway.sql_table
        q = self.rel_model(owner)._gateway.base_query
        for f, v in zip(rel_field, val):
            q = q.where(t.__getattr__(f) == v)
        return q
