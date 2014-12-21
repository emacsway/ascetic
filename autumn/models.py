from __future__ import absolute_import, unicode_literals
import re
import copy
import collections
from sqlbuilder import smartsql
from . import signals
from .connections import get_db
from .utils import classproperty

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

    def to_python(self, value):
        return value

    def to_string(self, value):
        data_registry.convert_to_string(value)


class ModelOptions(object):
    """Model options"""

    pk = 'id'
    using = 'default'
    field_class = Field

    def __init__(self, model, **kw):
        """Instance constructor"""
        self.relations = {}

        for k, v in kw:
            setattr(self, k, v)

        self.model = model
        if not hasattr(self, 'name'):
            self.name = ".".join((self.model.__module__, self.model.__name__))
        if not hasattr(self, 'db_table'):
            self.db_table = "_".join([
                re.sub(r"[^a-z0-9]", "", i.lower())
                for i in (self.model.__module__.split(".") + [self.model.__name__, ])
            ])

        if not hasattr(self, 'validations'):
            self.validations = {}
        for k, v in self.validations.items():
            if not isinstance(v, (list, tuple)):
                self.validations[k] = (v, )

        db = get_db(self.using)

        schema = db.describe_table(self.db_table)
        map_ = dict([(v, k) for k, v in getattr(self, 'map', {}).items()])
        # fileds and columns can be a descriptor for multilingual mapping.

        self.declared_fields = {}
        for name in self.model.__dict__:
            field = getattr(self.model, name, None)
            if isinstance(field, Field):
                self.declared_fields[name] = field
                delattr(self.model, name)
                if getattr(field, 'column', None):
                    map_[field.column] = name

        # self.all(whole, total)_fields = collections.OrderedDict()  # with parents, MTI
        self.fields = collections.OrderedDict()
        self.columns = collections.OrderedDict()
        q = db.execute('SELECT * FROM {0} LIMIT 1'.format(qn(self.db_table)))
        # See cursor.description http://www.python.org/dev/peps/pep-0249/
        for row in q.description:
            column = row[0]
            name = map_.get(column, column)
            data = schema.get(column, {})
            data.update({'column': column, 'type_code': row[1]})
            if name in self.declared_fields:
                field = self.declared_fields.get(name)
                field.__dict__.update(data)
            else:
                field = self.field_class(**data)
            self.add_field(field, name)

    def add_field(self, field, name):
        field.name = name
        field.model = self.model
        if getattr(field, 'validators', None):
            self.validations[name] = field.validators
        self.fields[name] = field
        self.columns[field.column] = field


class ModelBase(type):
    """Metaclass for Model"""
    options_class = ModelOptions

    def __new__(cls, name, bases, attrs):

        new_cls = type.__new__(cls, name, bases, attrs)

        if name in ('Model', 'NewBase', ):
            return new_cls

        if getattr(attrs.get('Meta'), 'abstract', None):
            del new_cls.Meta
            return new_cls

        if hasattr(new_cls, 'Meta'):
            if isinstance(new_cls.Meta, new_cls.options_class):
                NewOptions = new_cls.Meta
            else:
                class NewOptions(new_cls.Meta, new_cls.options_class):
                    pass
        else:
            NewOptions = new_cls.options_class
        opts = new_cls._meta = NewOptions(new_cls)

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

    _s = None

    def __init__(self, *args, **kwargs):
        """Allows setting of fields using kwargs"""
        self._send_signal(signal='pre_init', args=args, kwargs=kwargs, using=self._meta.using)
        self._new_record = True
        self._changed = set()
        self._errors = {}
        self._cache = {}
        self.__dict__[self._meta.pk] = None
        [setattr(self, self._meta.fields.keys()[i], arg) for i, arg in enumerate(args)]
        [setattr(self, k, v) for k, v in list(kwargs.items())]
        self._send_signal(signal='post_init', using=self._meta.using)

    def __setattr__(self, name, value):
        """Records when fields have changed"""
        if hasattr(getattr(type(self), name, None), '__set__'):
            return object.__setattr__(self, name, value)
        if name in self._meta.fields:
            field = self._meta.fields[name]
            self._changed.add(name)
            value = field.to_python(value)
        self.__dict__[name] = value

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._get_pk() == other._get_pk()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._get_pk())

    def _get_pk(self):
        """Sets the current value of the primary key"""
        return getattr(self, self._meta.pk, None)

    def _set_pk(self, value):
        """Sets the primary key"""
        return setattr(self, self._meta.pk, value)

    pk = property(_get_pk, _set_pk)

    def _set_defaults(self):
        """Sets attribute defaults based on ``defaults`` dict"""
        for k, v in list(getattr(self._meta, 'defaults', {}).items()):
            if getattr(self, k, None) is None:
                if isinstance(v, collections.Callable):
                    try:
                        v(self, k)
                    except TypeError:
                        v = v()
                setattr(self, k, v)

    def is_valid(self, exclude=frozenset(), fields=frozenset()):
        """Returns boolean on whether all ``validations`` pass"""
        self._validate(exclude, fields)
        return not self._errors

    def _validate(self, exclude=frozenset(), fields=frozenset()):
        """Tests all ``validations``"""
        self._set_defaults()
        self._errors = {}
        for key, validators in self._meta.validations.items():
            if key in exclude or (fields and key not in fields):
                continue
            for validator in validators:
                assert isinstance(validator, collections.Callable), 'The validator must be callable'
                value = getattr(self, key)
                if key == '__model__':
                    valid_or_msg = validator(self)
                else:
                    try:
                        valid_or_msg = validator(self, key, value)
                    except TypeError:
                        valid_or_msg = validator(value)
                if valid_or_msg is not True:
                    # Don't need message code. To rewrite message simple wrap (or extend) validator.
                    self._errors.setdefault(key, []).append(
                        valid_or_msg or 'Improper value "{0}" for "{1}"'.format(value, key)
                    )

    def _set_data(self, data):
        for column, value in data.items():
            try:
                attr = self._meta.columns[column].name
            except KeyError:
                attr = column
            setattr(self, attr, value)
        self._new_record = False
        self._changed = set()
        # Do use this method for sets File fields and other special data types?
        return self

    def _get_data(self, fields=frozenset(), exclude=frozenset()):
        return {f.column: getattr(self, f.name, None)
                for f in self._meta.fields.values()
                if not (f.name in exclude or (fields and f.name not in fields))}

    def save(self, using=None):
        """Sets defaults, validates and inserts into or updates database"""
        using = using or self._meta.using
        if not self.is_valid(fields=self._changed):
            raise self.ValidationError(self._errors)
        self._send_signal(signal='pre_save', update_fields=self._changed, using=using)
        result = self._insert(using) if self._new_record else self._update(using)
        self._send_signal(signal='post_save', created=self._new_record, update_fields=self._changed, using=using)
        self._new_record = False
        self._changed = set()
        return result

    def _insert(self, using):
        """Uses SQL INSERT to create new record"""
        auto_pk = self._get_pk() is None
        exclude = set([self._meta.pk]) if auto_pk else set()
        cursor = type(self).qs.using(using).insert(self._get_data(exclude=exclude))
        if auto_pk:
            self._set_pk(type(self).qs.db.last_insert_id(cursor))
        return True

    def _update(self, using):
        """Uses SQL UPDATE to update record"""
        type(self).qs.using(using).where(type(self).s.pk == self.pk).update(self._get_data(fields=self._changed))

    def delete(self, using=None):
        """Deletes record from database"""
        using = using or self._meta.using
        self._send_signal(signal='pre_delete', using=using)
        for key, rel in self._meta.relations.items():
            if isinstance(rel, OneToMany):
                for child in getattr(self, key).iterator():
                    rel.on_delete(self, child, rel)
            elif isinstance(rel, OneToOne):
                rel.on_delete(self, getattr(self, key), rel)
        type(self).qs.using(using).where(type(self).s.pk == self.pk).delete()
        self._send_signal(signal='post_delete', using=using)
        return True

    def serialize(self, fields=frozenset(), exclude=frozenset()):
        self._set_defaults()
        result = {}
        for field in self._meta.fields.values():
            if field.name in exclude or (fields and field.name not in fields):
                continue
            result[fields.name] = field.to_string(getattr(self, field.name, None))
        return result

    def _send_signal(self, *a, **kw):
        """Sends signal"""
        kw.update({'sender': type(self), 'instance': self})
        return signals.send_signal(*a, **kw)

    @classproperty
    def s(cls):
        if '_s' not in cls.__dict__:
            cls._s = Table(cls)
        return cls._s

    @classproperty
    def qs(cls):
        return cls.s.qs

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

    class ValidationError(Exception):
        pass

    def __repr__(self):
        return "<{0}.{1}: {2}>".format(type(self).__module__, type(self).__name__, self.pk)


class DataRegistry(object):
    """
    Stores data convertors
    """
    def __init__(self):
        """Constructor, initial registry."""
        self._register = {}

    def register(self, *args):
        """Registers callbacks."""
        def decorator(func):
            self._register[args] = func
            return func
        return decorator

    def to_python(self, dialect, code):
        return self.register('to_python', dialect, code)

    def to_sql(self, dialect, type):
        return self.register('to_sql', dialect, type)

    def to_string(self, type):
        return self.register('to_string', type)

    def convert_to_python(self, dialect, code, value):
        try:
            convertor = self._register[('to_python', dialect, code)]
        except KeyError:
            return value
        else:
            return convertor(value)

    def convert_to_sql(self, dialect, value):
        for t in type(value).mro():
            try:
                convertor = self._register[('to_sql', dialect, t)]
            except KeyError:
                pass
            else:
                return convertor(value)
        return value

    def convert_to_string(self, dialect, value):
        for t in type(value).mro():
            try:
                convertor = self._register[('to_string', t)]
            except KeyError:
                pass
            else:
                return convertor(value)
        return value


@cr('QuerySet')
class QS(smartsql.QS):
    """Query Set adapted."""

    _expr = None
    _cache = None
    _using = 'default'
    model = None

    def __init__(self, tables=None):
        super(QS, self).__init__(tables=tables)
        self._prefetch = {}
        self.is_base(True)
        if isinstance(tables, Table):
            self.model = tables.model
            self._using = self.model._meta.using
            self._set_dialect()

    def raw(self, sql, *params):
        self = self.clone()
        self._expr = smartsql.OmitParentheses(smartsql.E(sql, *params))
        return self

    def clone(self):
        c = super(QS, self).clone()
        c._cache = None
        c._prefetch = self._prefetch.copy()
        c.is_base(False)
        return c

    def __len__(self):
        """Returns length or list."""
        self.fill_cache()
        return len(self._cache)

    def count(self):
        """Returns length or list."""
        if self._cache is not None:
            return len(self._cache)
        return super(QS, self).count()

    def fill_cache(self):
        if self.is_base():
            raise Exception('You should clone base queryset before query.')
        if self._cache is None:
            self._cache = list(self.iterator())
            self.populate_prefetch()
        return self

    def populate_prefetch(self):
        for key, qs in self._prefetch.items():
            rel = self.model._meta.relations[key]
            # recursive handle prefetch
            rows = list(qs.where(
                smartsql.Field(rel.rel_field).in_(
                    filter(None, [getattr(i, rel.field) for i in self._cache])
                )
            ))
            for obj in self._cache:
                val = [i for i in rows if getattr(i, rel.rel_field) == getattr(obj, rel.field)]
                if isinstance(rel, (ForeignKey, OneToOne)):
                    val = val[0] if val else None
                    if val and isinstance(rel, OneToOne):
                        setattr(val, "{}_prefetch".format(rel.rel_name), obj)
                elif isinstance(rel, OneToMany):
                    for i in val:
                        setattr(i, "{}_prefetch".format(rel.rel_name), obj)
                setattr(obj, "{}_prefetch".format(key), val)

    def prefetch(self, *a, **kw):
        """Prefetch relations"""
        c = self.clone()
        if a and not a[0]:
            c._prefetch = {}
        else:
            c._prefetch.update(kw)
            c._prefetch.update({i: self.model._meta.relations[i].qs for i in a})
        return c

    def __iter__(self):
        """Returns iterator."""
        self.fill_cache()
        return iter(self._cache)

    def iterator(self):
        """iterator"""
        cursor = self.execute()
        fields = []
        for f in cursor.description:
            fn_suf = fn = f[0]
            c = 2
            while fn_suf in fields:
                fn_suf = fn + str(c)
                c += 1
            fields.append(fn_suf)

        for row in cursor.fetchall():
            row = list(row)
            for i, v in enumerate(row[:]):
                row[i] = data_registry.convert_to_python(self.dialect(), cursor.description[i][1], v)
            data = dict(list(zip(fields, row)))
            yield self.model()._set_data(data) if self.model else data

    def __getitem__(self, key):
        """Returns sliced self or item."""
        if self._cache:
            return self._cache[key]
        if isinstance(key, integer_types):
            self = super(QS, self).__getitem__(key)
            return list(self)[0]
        return super(QS, self).__getitem__(key)

    def _build_sql(self):
        if not (self._action == "select" and self._expr):
            return super(QS, self)._build_sql()
        sql = smartsql.ExprList(self._expr)
        if self._limit:
            sql.append(self._limit)
        return sql

    def using(self, alias=None):
        if alias is None:
            return self._using
        self = self.clone()
        self._using = alias
        self._set_dialect()
        return self

    def _set_dialect(self):
        engine = self.db.engine
        SMARTSQL_DIALECTS.get(engine, engine)
        self._dialect = SMARTSQL_DIALECTS.get(engine, engine)

    def dialect(self):
        return self._dialect

    def is_base(self, value=None):
        if value is None:
            return self._is_base
        self._is_base = value
        return self

    def sqlrepr(self, expr=None):
        return smartsql.sqlrepr(expr or self, self.dialect())

    def sqlparams(self, expr=None):
        params = smartsql.sqlparams(expr or self)
        for i, v in enumerate(params[:]):
            params[i] = data_registry.convert_to_sql(self.dialect(), v)
        return params

    def execute(self):
        """Implementation of query execution"""
        sql = self._build_sql()
        return self._execute(self.sqlrepr(sql), self.sqlparams(sql))

    def _execute(self, sql, params):
        return self.db.execute(sql, params)

    def result(self):
        """Result"""
        if self._action == 'select':
            return self
        if self._action == 'count':
            return self.execute().fetchone()[0]
        return self.execute()

    @property
    def db(self):
        return get_db(self.using())


@cr
class UnionQuerySet(smartsql.UnionQuerySet, QS):
    """Union query class"""
    def __init__(self, qs):
        super(UnionQuerySet, self).__init__(qs)
        self.model = qs.model
        self._using = qs.using()


@cr
class Table(smartsql.Table):
    """Table class"""

    def __init__(self, model, qs=None, *args, **kwargs):
        """Constructor"""
        super(Table, self).__init__(model._meta.db_table, *args, **kwargs)
        self.model = model
        self._qs = qs

    def _get_qs(self):
        if isinstance(self._qs, collections.Callable):
            self._qs = self._qs(self)
        elif self._qs is None:
            self._qs = QS(self).fields(self.get_fields())
        return self._qs

    def _set_qs(self, val):
        self._qs = val

    qs = property(_get_qs, _set_qs)

    def get_fields(self, prefix=None):
        """Returns field list."""
        return [smartsql.Field(f.column, prefix if prefix is not None else self) for f in self.model._meta.fields.values()]

    def __getattr__(self, name):
        """Added some specific functional."""
        if name[0] == '_':
            raise AttributeError
        parts = name.split(smartsql.LOOKUP_SEP, 1)
        field = parts[0]
        result = {'field': field, }
        signals.send_signal(signal='field_conversion', sender=self, result=result, field=field, model=self.model)
        field = result['field']
        if field == 'pk':
            field = self.model._meta.pk
        if isinstance(self.model._meta.relations.get(field, None), ForeignKey):
            field = self.model._meta.relations.get(field).field
        if field in self.model._meta.fields:
            field = self.model._meta.fields[field].column
        parts[0] = field
        return super(Table, self).__getattr__(smartsql.LOOKUP_SEP.join(parts))


@cr
class TableAlias(smartsql.TableAlias, Table):
    """Table alias class"""
    @property
    def model(self):
        return getattr(self._table, 'model', None)  # Can be subquery


def qn(name, using='default'):
    """Quotes DB name"""
    engine = get_db(using).engine
    return smartsql.qn(name, SMARTSQL_DIALECTS.get(engine, engine))


def cascade(parent, child, parent_rel):
    child.delete()


def set_null(parent, child, parent_rel):
    setattr(child, parent_rel.rel_field, None)
    child.save()


def do_nothing(parent, child, parent_rel):
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
            self._qs = self.rel_model.s.qs
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
        fk_val = getattr(instance, self.field)
        if fk_val is None:
            return None
        if getattr(instance._cache.get(self.name, None), self.rel_field, None) != fk_val:
            instance._cache[self.name] = self.filter(**{self.rel_field: fk_val})[0]
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
        setattr(instance, self.field, value)

    def __delete__(self, instance):
        instance._cache.pop(self.name, None)
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
        # Cache attr already exists in QS, so, can be even setable.
        return self.filter(**{self.rel_field: getattr(instance, self.field)})


data_registry = DataRegistry()


@data_registry.to_sql('sqlite', Model)
@data_registry.to_sql('mysql', Model)
@data_registry.to_sql('postgres', Model)
def model_to_sql(val):
    return val.pk
