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

    def add(self, model):
        self[model._meta.name] = model

    def __getitem__(self, model_name):
        try:
            return self[model_name]
        except KeyError:
            raise ModelNotRegistered

registry = ModelRegistry()


class Field(object):
    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


class TableResult(smartsql.Result):
    """Result adapted for table."""

    gateway = None
    _raw = None
    _cache = None
    _using = 'default'

    def __init__(self, gateway):
        self._prefetch = {}
        self._select_related = {}
        self.is_base(True)
        self._mapping = default_mapping
        self.gateway = gateway
        self._using = gateway.using

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
            self._query = super(TableResult, self).__getitem__(key)
            return list(self)[0]
        return super(TableResult, self).__getitem__(key)

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

    def using(self, alias=None):
        if alias is None:
            return self._using
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


class TableGateway(object):
    """Model options"""

    pk = 'id'
    using = 'default'
    field_factory = Field
    result_factory = TableResult

    def __init__(self, db_table=None, **kw):
        """Instance constructor"""

        for k, v in kw:
            setattr(self, k, v)

        if db_table:
            self.db_table = db_table

        self.declared_fields = self.create_declared_fields(
            getattr(self, 'map', {}),
            getattr(self, 'validations', {}),
            getattr(self, 'declared_fields', {})
        )
        # fileds and columns can be a descriptor for multilingual mapping.
        self.fields = collections.OrderedDict()
        self.columns = collections.OrderedDict()

        for name, field in self.create_fields(self.read_columns(self.db_table, self.using), self.declared_fields).items():
            self.add_field(name, field)

        self.sql_table = self.create_sql_table()
        self.query = self.create_query()

    def create_declared_fields(self, map, validations, declared_fields):
        result = {}

        for name, column in map.items():
            result[name] = self.create_field(name, {'column': column}, declared_fields)

        for name, validators in validations.items():
            if not isinstance(validators, (list, tuple)):
                validators = [validators, ]
            result[name] = self.create_field(name, {'validators': validators}, declared_fields)

        return result

    def read_columns(self, db_table, using):
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
        field.model = self.model
        self.fields[name] = field
        self.columns[field.column] = field

    def create_sql_table(self):
        return smartsql.Table(self.db_table)

    def create_query(self):
        return smartsql.QS(self.sql_table, result=self.result_factory(self)).fields(self.get_sql_fields())

    def get_sql_fields(self, prefix=None):
        """Returns field list."""
        return [smartsql.Field(f.column, prefix if prefix is not None else self.sql_table) for f in self.fields.values()]

    def insert(self, data, using=None):
        using = using or self.using
        pk = self.pk if type(self.pk) == tuple else (self.pk,)
        auto_pk = all(*(data.get(k) for k in pk))
        if auto_pk:
            data = {k: v for k, v in data.items() if k not in pk}
        cursor = self.query.using(using).insert(dict(self._get_data()))
        if auto_pk:
            self._set_pk(type(self).qs.result.db.last_insert_id(cursor))
        return True


class ModelResultMixIn(object):
    """Result adapted for model."""

    def fill_cache(self):
        self = super(ModelResultMixIn, self).fill_cache()
        self.populate_prefetch()
        return self

    def prefetch(self, *a, **kw):
        """Prefetch relations"""
        if a and not a[0]:  # .prefetch(False)
            self._prefetch = {}
        else:
            self._prefetch = copy.copy(self._prefetch)
            self._prefetch.update(kw)
            self._prefetch.update({i: self.gateway.relations[i].qs for i in a})
        return self

    def populate_prefetch(self):
        for key, qs in self._prefetch.items():
            rel = self.gateway.relations[key]
            # recursive handle prefetch
            field = rel.field if type(rel.field) == tuple else (rel.field,)
            rel_field = rel.rel_field if type(rel.rel_field) == tuple else (rel.rel_field,)

            cond = reduce(operator.or_,
                          (reduce(operator.and_,
                                  ((smartsql.Field(rf, rel.rel_model.s) == getattr(obj, f))
                                   for f, rf in zip(field, rel_field)))
                           for obj in self._cache))
            rows = list(qs.where(cond))
            for obj in self._cache:
                val = [i for i in rows if tuple(getattr(i, f) for f in rel_field) == tuple(getattr(obj, f) for f in field)]
                if isinstance(rel, (ForeignKey, OneToOne)):
                    val = val[0] if val else None
                    if val and isinstance(rel, OneToOne):
                        setattr(val, "{}_prefetch".format(rel.rel_name), obj)
                elif isinstance(rel, OneToMany):
                    for i in val:
                        setattr(i, "{}_prefetch".format(rel.rel_name), obj)
                setattr(obj, "{}_prefetch".format(key), val)


class ModelGatewayMixIn(object):

    def __init__(self, model, **kw):
        """Instance constructor"""
        self.model = model
        self.relations = {}

        if not hasattr(self, 'name'):
            self.name = self.create_default_name(model)

        self.declared_fields = self.create_model_declared_fields(model)

        db_table = getattr(self, 'db_table', None) or self.create_default_db_table(model)
        super(ModelGatewayMixIn, self).__init__(db_table=db_table, **kw)

    def create_default_name(self, model):
        return ".".join((model.__module__, model.__name__))

    def create_default_db_table(self, model):
        return "_".join([
            re.sub(r"[^a-z0-9]", "", i.lower())
            for i in (self.model.__module__.split(".") + [self.model.__name__, ])
        ])

    def create_model_declared_fields(self, model):
        declared_fields = {}
        # TODO: FIXME for inheritance
        for name in dir(self.model):
            field = getattr(self.model, name, None)
            if isinstance(field, Field):
                declared_fields[name] = field
                delattr(self.model, name)

        return declared_fields

    def add_field(self, name, field):
        field.model = self.model
        super(ModelGatewayMixIn, self).add_field(name, field)

    def create_sql_table(self):
        return Table(self)


class ModelResult(ModelResultMixIn, TableResult):
    pass


class ModelGateway(ModelGatewayMixIn, TableGateway):
    result_factory = ModelResult


class ModelBase(type):
    """Metaclass for Model"""
    gateway_class = ModelGateway

    def __new__(cls, name, bases, attrs):

        new_cls = type.__new__(cls, name, bases, attrs)

        if name in ('Model', 'NewBase', ):
            return new_cls

        if getattr(attrs.get('Meta'), 'abstract', None):
            del new_cls.Meta
            return new_cls

        if hasattr(new_cls, 'Meta'):
            if isinstance(new_cls.Meta, new_cls.gateway_class):
                NewGateway = new_cls.Meta
            else:
                class NewGateway(new_cls.Meta, new_cls.gateway_class):
                    pass
        else:
            NewGateway = new_cls.gateway_class
        new_cls._meta = NewGateway(new_cls)

        for key, rel in new_cls.__dict__.items():
            if isinstance(rel, Relation):
                rel.add_to_class(new_cls, key)

        registry.add(new_cls)

        for m in registry.values():
            for key, rel in m._meta.relations.items():
                try:
                    if hasattr(rel, 'add_related') and rel.rel_model is new_cls:
                        rel.add_related()
                except ModelNotRegistered:
                    pass

        signals.send_signal(signal='class_prepared', sender=new_cls, using=new_cls._meta.using)
        return new_cls


class Model(ModelBase(b"NewBase", (object, ), {})):
    """Model class"""

    _new_record = True
    _s = None

    def __init__(self, *args, **kwargs):
        """Allows setting of fields using kwargs"""
        self._send_signal(signal='pre_init', args=args, kwargs=kwargs, using=self._meta.using)
        self._cache = {}
        pk = self._meta.pk
        if type(pk) == tuple:
            for k in pk:
                self.__dict__[k] = None
        else:
            self.__dict__[pk] = None
        if args:
            for i, arg in enumerate(args):
                setattr(self, self._meta.fields.keys()[i], arg)
        if kwargs:
            for k, v in kwargs.items():
                setattr(self, k, v)
        self._send_signal(signal='post_init', using=self._meta.using)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._get_pk() == other._get_pk()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._get_pk())

    def _get_pk(self):
        """Sets the current value of the primary key"""
        pk = self._meta.pk
        if type(pk) == tuple:
            return tuple(getattr(self, k, None) for k in pk)
        return getattr(self, pk, None)

    def _set_pk(self, value):
        """Sets the primary key"""
        pk = self._meta.pk
        if type(pk) == tuple:
            for k, v in zip(pk, value):
                setattr(self, k, v)
        else:
            setattr(self, self._meta.pk, value)

    pk = property(_get_pk, _set_pk)

    def _set_defaults(self):
        """Sets attribute defaults based on ``defaults`` dict"""
        for k, v in getattr(self._meta, 'defaults', {}).items():
            if getattr(self, k, None) is None:
                if isinstance(v, collections.Callable):
                    try:
                        v(self, k)
                    except TypeError:
                        v = v()
                setattr(self, k, v)

    def validate(self, exclude=frozenset(), fields=frozenset()):
        """Tests all ``validations``"""
        self._set_defaults()
        errors = {}
        for name, field in self._meta.fields.items():
            if name in exclude or (fields and name not in fields):
                continue
            if not hasattr(field, 'validators'):
                continue
            for validator in field.validators:
                assert isinstance(validator, collections.Callable), 'The validator must be callable'
                value = getattr(self, name)
                try:
                    valid_or_msg = validator(self, name, value)
                except TypeError:
                    valid_or_msg = validator(value)
                if valid_or_msg is not True:
                    # Don't need message code. To rewrite message simple wrap (or extend) validator.
                    errors.setdefault(name, []).append(
                        valid_or_msg or 'Improper value "{0}" for "{1}"'.format(value, name)
                    )
        if errors:
            raise ValidationError(errors)

    def _set_data(self, data):
        for column, value in data:
            try:
                attr = self._meta.columns[column].name
            except KeyError:
                attr = column
            self.__dict__[attr] = value
        self._new_record = False
        # Do use this method for sets File fields and other special data types?
        return self

    def _get_data(self, fields=frozenset(), exclude=frozenset()):
        return tuple((f.column, getattr(self, f.name, None))
                     for f in self._meta.fields.values()
                     if not (f.name in exclude or (fields and f.name not in fields)))

    def save(self, using=None):
        """Sets defaults, validates and inserts into or updates database"""
        self._set_defaults()
        using = using or self._meta.using
        self._send_signal(signal='pre_save', using=using)
        result = self._insert(using) if self._new_record else self._update(using)
        self._send_signal(signal='post_save', created=self._new_record, using=using)
        self._new_record = False
        return result

    def _insert(self, using):
        """Uses SQL INSERT to create new record"""
        auto_pk = self._get_pk() is None
        exclude = set([self._meta.pk]) if auto_pk else set()
        cursor = type(self).qs.using(using).insert(dict(self._get_data(exclude=exclude)))
        if auto_pk:
            self._set_pk(type(self).qs.result.db.last_insert_id(cursor))
        return True

    def _update(self, using):
        """Uses SQL UPDATE to update record"""
        pk = (self)._meta.pk
        if type(pk) == tuple:
            cond = reduce(operator.and_, (getattr(type(self).s, k) == v for k, v in zip(pk, self.pk)))
        else:
            cond = self.s.pk == self.pk
        type(self).qs.using(using).where(
            cond
        ).update(dict(self._get_data()))

    def delete(self, using=None, visited=None):
        """Deletes record from database"""
        using = using or self._meta.using

        if visited is None:
            visited = set()
        if self in visited:
            return False
        visited.add(self)

        self._send_signal(signal='pre_delete', using=using)
        for key, rel in self._meta.relations.items():
            if isinstance(rel, OneToMany):
                for child in getattr(self, key).iterator():
                    rel.on_delete(self, child, rel, visited)
            elif isinstance(rel, OneToOne):
                child = getattr(self, key)
                rel.on_delete(self, child, rel, visited)

        pk = (self)._meta.pk
        if type(pk) == tuple:
            cond = reduce(operator.and_, (getattr(self.s, k) == v for k, v in zip(pk, self.pk)))
        else:
            cond = type(self).s.pk == self.pk

        type(self).qs.using(using).where(cond).delete()
        self._send_signal(signal='post_delete', using=using)
        return True

    def _send_signal(self, *a, **kw):
        """Sends signal"""
        kw.update({'sender': type(self), 'instance': self})
        return signals.send_signal(*a, **kw)

    @classproperty
    def s(cls):
        # TODO: Use Model class descriptor without setter.
        return cls._meta.sql_table

    @classproperty
    def qs(cls):
        return cls._meta.query

    @classmethod
    def get(cls, _obj_pk=None, **kwargs):
        'Returns QS object'
        if _obj_pk is not None:
            return cls.get(**{cls._meta.pk: _obj_pk})[0]

        if kwargs:
            qs = cls.qs
            for k, v in kwargs.items():
                qs = qs.where(smartsql.Field(k, cls.s) == v)
            return qs

        return cls.qs.clone()

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
    try:
        return result.gateway.model()._set_data(row)
    except AttributeError:
        dict(row)


class RelatedMapping(object):

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
            model_row_dict = dict(model_row)
            pk = model._meta.pk
            if type(pk) != tuple:
                pk = (pk,)
            key = (model, tuple(model_row_dict[f] for f in pk))
            if key not in state:
                state[key] = model()._set_data(model_row)
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
        objs = self.get_objects(models, rows)
        self.build_relations(relations, objs)
        return objs[0]


@cr
class Table(smartsql.Table):
    """Table class"""

    def __init__(self, gateway, qs=None, *args, **kwargs):
        """Constructor"""
        super(Table, self).__init__(gateway.db_table, *args, **kwargs)
        self.gateway = gateway

    @property
    def qs(self):
        return self.gateway.query

    def get_fields(self, prefix=None):
        """Returns field list."""
        return self.gateway.get_sql_fields()

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
            field = self.gateway.pk
        elif isinstance(self.gateway.relations.get(field, None), ForeignKey):
            field = self.gateway.relations.get(field).field

        if type(field) == tuple:
            if len(parts) > 1:
                raise Exception("Can't set single alias for multiple fields of composite key {}.{}".format(self.model, name))
            return smartsql.CompositeExpr(*(self.__getattr__(k) for k in field))

        if field in self.gateway.fields:
            field = self.gateway.fields[field].column
        parts[0] = field
        return super(Table, self).__getattr__(smartsql.LOOKUP_SEP.join(parts))


def cascade(parent, child, parent_rel, visited):
    child.delete(visited=visited)


def set_null(parent, child, parent_rel, visited):
    setattr(child, parent_rel.rel_field, None)
    child.save()


def do_nothing(parent, child, parent_rel, visited):
    pass

# TODO: descriptor for FileField? Or custom postgresql data type? See http://www.postgresql.org/docs/8.4/static/sql-createtype.html


class Relation(object):

    def __init__(self, rel_model, rel_field=None, field=None, qs=None, on_delete=cascade, rel_name=None, related_name=None):
        self.rel_model_or_name = rel_model
        self._rel_field = rel_field
        self._field = field
        self._qs = qs
        self.on_delete = on_delete
        self._rel_name = rel_name
        if related_name:
            smartsql.warn('related_name', 'rel_name')
            self._rel_name = self._rel_name or related_name

    def add_to_class(self, model_class, name):
        self.model = model_class
        self.name = name
        self.model._meta.relations[name] = self
        setattr(self.model, name, self)

    @property
    def rel_model(self):
        if isinstance(self.rel_model_or_name, string_types):
            name = self.rel_model_or_name
            if name == 'self':
                name = self.model._meta.name
            return registry[name]
        return self.rel_model_or_name

    def _get_qs(self):
        if isinstance(self._qs, collections.Callable):
            self._qs = self._qs(self)
        elif self._qs is None:
            self._qs = self.rel_model._meta.query
        return self._qs.clone()

    def _set_qs(self, val):
        self._qs = val

    qs = property(_get_qs, _set_qs)

    def filter(self, *a, **kw):
        qs = self.qs
        t = self.rel_model.s
        for fn, param in kw.items():
            f = smartsql.Field(fn, t)
            qs = qs.where(f == param)
        return qs


class ForeignKey(Relation):

    @property
    def field(self):
        return self._field or '{0}_id'.format(self.rel_model._meta.db_table.rsplit("_", 1).pop())

    @property
    def rel_field(self):
        return self._rel_field or self.rel_model._meta.pk

    @property
    def rel_name(self):
        return self._rel_name or '{0}_set'.format(self.rel_model.__name__.lower())

    def add_to_class(self, model_class, name):
        super(ForeignKey, self).add_to_class(model_class, name)
        self.add_related()

    def add_related(self):
        try:
            rel_model = self.rel_model
        except ModelNotRegistered:
            return

        if self.rel_name in rel_model._meta.relations:
            return

        OneToMany(
            self.model, self.field, self.rel_field,
            None, on_delete=self.on_delete, rel_name=self.name
        ).add_to_class(
            rel_model, self.rel_name
        )

    def __get__(self, instance, owner):
        if not instance:
            return self
        field = self.field if type(self.field) == tuple else (self.field,)
        rel_field = self.rel_field if type(self.rel_field) == tuple else (self.rel_field,)
        fk_val = tuple(getattr(instance, f) for f in field)
        if not [i for i in fk_val if i is not None]:
            return None

        if (getattr(instance._cache.get(self.name, None), f, None) for f in self.rel_field) != fk_val:
            instance._cache[self.name] = self.filter(**dict(zip(rel_field, fk_val)))[0]
        return instance._cache[self.name]

    def __set__(self, instance, value):
        if isinstance(value, Model):
            if not isinstance(value, self.rel_model):
                raise Exception(
                    ('Value should be an instance of "{0}" ' +
                     'or primary key of related instance.').format(
                        self.rel_model._meta.name
                    )
                )
            instance._cache[self.name] = value
            value = value._get_pk()
        if type(self.field) == tuple:
            for a, v in zip(self.field, value):
                setattr(instance, a, v)
        else:
            setattr(instance, self.field, value)

    def __delete__(self, instance):
        instance._cache.pop(self.name, None)
        if type(self.field) == tuple:
            for a in self.field:
                setattr(instance, a, None)
        else:
            setattr(instance, self.field, None)


class OneToOne(ForeignKey):

    def add_related(self):
        try:
            rel_model = self.rel_model
        except ModelNotRegistered:
            return

        if self.rel_name in rel_model._meta.relations:
            return

        OneToOne(
            self.model, self.field, self.rel_field,
            None, on_delete=self.on_delete, rel_name=self.name
        ).add_to_class(
            rel_model, self.rel_name
        )
        self.on_delete = do_nothing


class OneToMany(Relation):

    # TODO: is it need add_related() here to construct related FK?

    @property
    def field(self):
        return self._field or self.model._meta.pk

    @property
    def rel_field(self):
        return self._rel_field or '{0}_id'.format(self.model._meta.db_table.rsplit("_", 1).pop())

    @property
    def rel_name(self):
        return self._rel_name or self.rel_model.__name__.lower()

    def __get__(self, instance, owner):
        if not instance:
            return self
        field = self.field if type(self.field) == tuple else (self.field,)
        rel_field = self.rel_field if type(self.rel_field) == tuple else (self.rel_field,)
        val = tuple(getattr(instance, f) for f in field)
        # Cache attr already exists in QS, so, can be even setable.
        return self.filter(**dict(zip(rel_field, val)))
