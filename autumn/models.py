from __future__ import absolute_import, unicode_literals
import re
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


class ModelNotRegistered(Exception):
    pass


class ModelRegistry(object):
    models = {}

    def add(self, model):
        self.models[model._meta.name] = model

    def get(self, model_name):
        try:
            return self.models[model_name]
        except KeyError:
            raise ModelNotRegistered

registry = ModelRegistry()


class ModelOptions(object):
    """Model options"""

    pk = 'id'
    using = 'default'

    def __init__(self, model, **kw):
        """Instance constructor"""
        self.relations = {}

        for k, v in kw:
            setattr(self, k, v)

        self.model = model
        if not hasattr(self, 'name'):
            self.name = ".".join((model.__module__, model.__name__))
        if not hasattr(self, 'db_table'):
            self.db_table = "_".join([
                re.sub(r"[^a-z0-9]", "", i.lower())
                for i in (self.model.__module__.split(".") + [self.model.__name__, ])
            ])

        for k, v in list(getattr(self, 'validations', {}).items()):
            if not isinstance(v, (list, tuple)):
                self.validations[k] = (v, )

        # See cursor.description http://www.python.org/dev/peps/pep-0249/
        db = get_db(self.using)

        self.schema = {}
        if hasattr(db, 'describe_table'):
            self.schema = db.describe_table(self.db_table)

        q = db.execute('SELECT * FROM {0} LIMIT 1'.format(qn(self.db_table)))
        self.fields = [f[0] for f in q.description]
        for f in q.description:
            self.schema.setdefault(f[0], {})['type_code'] = f[1]


class ModelBase(type):
    """Metaclass for Model"""
    def __new__(cls, name, bases, attrs):
        if name in ('Model', 'NewBase', ):
            return super(ModelBase, cls).__new__(cls, name, bases, attrs)

        new_cls = type.__new__(cls, name, bases, attrs)

        if hasattr(new_cls, 'Meta'):
            class NewOptions(new_cls.Meta, ModelOptions):
                pass
        else:
            NewOptions = ModelOptions
        opts = new_cls._meta = NewOptions(new_cls)

        for key, rel in new_cls.__dict__.items():
            if isinstance(rel, Relation):
                rel.add_to_class(new_cls, key)

        for b in bases:
            if not hasattr(b, '_meta'):
                continue
            base_meta = getattr(b, '_meta')
            # TODO: inheritable options???
            # May be better way is a Meta class inheritance?

        registry.add(new_cls)

        for m in registry.models.values():
            for key, rel in m._meta.relations.items():
                try:
                    if hasattr(rel, 'add_related') and rel.rel_model is new_cls:
                        rel.add_related()
                except ModelNotRegistered:
                    pass

        signals.send_signal(signal='class_prepared', sender=new_cls, using=new_cls._meta.using)
        return new_cls


class Model(ModelBase(bytes("NewBase"), (object, ), {})):
    """Model class"""

    _s = None

    def __init__(self, *args, **kwargs):
        """Allows setting of fields using kwargs"""
        self._send_signal(signal='pre_init', args=args, kwargs=kwargs)
        self.__dict__[self._meta.pk] = None
        self._new_record = True
        [setattr(self, self._meta.fields[i], arg) for i, arg in enumerate(args)]
        [setattr(self, k, v) for k, v in list(kwargs.items())]
        self._changed = set()
        self._send_signal(signal='post_init')
        self._errors = {}

    def __setattr__(self, name, value):
        """Records when fields have changed"""
        cls_attr = getattr(type(self), name, None)
        if cls_attr is not None:
            if isinstance(cls_attr, property) or issubclass(cls_attr, Model):
                return object.__setattr__(self, name, value)
        if name != '_changed' and name in self._meta.fields and hasattr(self, '_changed'):
            self._changed.add(name)
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

    def _get_defaults(self):
        """Sets attribute defaults based on ``defaults`` dict"""
        for k, v in list(getattr(self._meta, 'defaults', {}).items()):
            if not getattr(self, k, None):
                if isinstance(v, collections.Callable):
                    v = v()
                setattr(self, k, v)

    def is_valid(self, skip=()):
        """Returns boolean on whether all ``validations`` pass"""
        self._validate(skip)
        return not self._errors

    def _validate(self, skip=()):
        """Tests all ``validations``"""
        self._errors = {}
        for key, validators in list(getattr(self._meta, 'validations', {}).items()):
            if key in skip:
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
                    self._errors.setdefault(key, []).append(
                        valid_or_msg or 'Improper value "{0}" for "{1}"'.format(value, key)
                    )

    def save(self):
        """Sets defaults, validates and inserts into or updates database"""
        self._get_defaults()
        if not self.is_valid():
            raise self.ValidationError("Invalid data!")
        created = self._new_record
        update_fields = self._changed
        self._send_signal(signal='pre_save', update_fields=update_fields)
        if self._new_record:
            self._insert()
            self._new_record = False
            result = True
        else:
            result = self._update()
        self._send_signal(signal='post_save', created=created, update_fields=update_fields)
        return result

    def _insert(self):
        """Uses SQL INSERT to create new record"""
        auto_pk = self._get_pk() is None
        fields = [f for f in self._meta.fields
                  if f != self._meta.pk or not auto_pk]
        params = [getattr(self, f, None) for f in self._meta.fields
                  if f != self._meta.pk or not auto_pk]
        cursor = type(self).qs.insert(dict(zip(fields, params)))

        if self._get_pk() is None:
            self._set_pk(type(self).qs.get_db().last_insert_id(cursor))
        return True

    def _update(self):
        """Uses SQL UPDATE to update record"""
        params = [getattr(self, f) for f in self._changed]
        type(self).qs.where(type(self).s.pk == self.pk).update(dict(zip(self._changed, params)))

    def delete(self):
        """Deletes record from database"""
        self._send_signal(signal='pre_delete')
        for key, rel in self._meta.relations.items():
            if isinstance(rel, OneToMany):
                for child in getattr(self, key).iterator():
                    rel.on_delete(self, child, rel)
        type(self).qs.where(type(self).s.pk == self.pk).delete()
        self._send_signal(signal='post_delete')
        return True

    def _send_signal(self, *a, **kw):
        """Sends signal"""
        kw.update({
            'sender': type(self),
            'instance': self,
            'using': self._meta.using,
        })
        return signals.send_signal(*a, **kw)

    @classproperty
    def s(cls):
        if cls._s is None:
            cls._s = Table(cls)
        return cls._s

    @classproperty
    def ss(cls):
        smartsql.warn('Model.ss', 'Model.s', 4)
        return cls.s

    @classproperty
    def qs(cls):
        return cls.s.qs

    @classmethod
    def get(cls, _obj_pk=None, **kwargs):
        'Returns QS object'
        if _obj_pk is not None:
            return cls.get(**{cls._meta.pk: _obj_pk})[0]

        qs = cls.qs
        for k, v in kwargs.items():
            qs = qs.where(smartsql.Field(k, cls.s) == v)
        return qs

    class ValidationError(Exception):
        pass


class DataRegistry(object):
    """
    Stores data convertors
    """
    def __init__(self):
        """Constructor, initial registry."""
        self._to_python = {}
        self._to_sql = {}

    def register(self, direction, dialect, code_or_type):
        """Registers callbacks."""
        def decorator(func):
            ns = getattr(self, '_{0}'.format(direction)).setdefault(dialect, {})
            ns[code_or_type] = func
            return func
        return decorator

    def to_python(self, dialect, code):
        return self.register('to_python', dialect, code)

    def to_sql(self, dialect, type):
        return self.register('to_sql', dialect, type)

    def convert_to_python(self, dialect, code, value):
        try:
            convertor = self._to_python[dialect][code]
        except KeyError:
            return value
        else:
            return convertor(value)

    def convert_to_sql(self, dialect, value):
        try:
            convertor = self._to_sql[dialect][type(value)]
        except KeyError:
            for t in self._to_sql.setdefault(dialect, {}).keys():
                if issubclass(type(value), t):
                    convertor = self._to_sql[dialect][t]
                    break
            else:
                return value
        return convertor(value)


class QS(smartsql.QS):
    """Query Set adapted."""

    _cache = None
    prefix_result = False
    model = None
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
            # TODO: variant init_fields = ((alias1, model1, model_field_list1), (alias2, model2, model_field_list2), ...)?
            # returns (instance of model1, instance of model2, another instance of model2, ...)
            # or instance.alias_name = other model instance.
            # How about fields from sub-select (not from table), that has not model?
            init_fields = self.get_init_fields()
            if len(fields) == len(init_fields):
                fields = init_fields

        for row in cursor.fetchall():
            row = list(row)
            for i, v in enumerate(row[:]):
                row[i] = data_registry.convert_to_python(
                    self.dialect(), cursor.description[i][1], v
                )
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
                if isinstance(f._prefix, Table) and getattr(f._prefix, 'model', None) == self.model:
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
        engine = self.get_db().engine
        return SMARTSQL_DIALECTS.get(engine, engine)

    def sqlrepr(self, expr=None):
        return smartsql.sqlrepr(expr or self, self.dialect())

    def sqlparams(self, expr=None):
        params = smartsql.sqlparams(expr or self)
        for i, v in enumerate(params[:]):
            params[i] = data_registry.convert_to_sql(
                self.dialect(), v
            )
        return params

    def execute(self):
        """Implementation of query execution"""
        if self._action in ('select', 'count', ):
            return self
        else:
            return self._execute(self.sqlrepr(), *self.sqlparams())

    def _execute(self, sql, *params):
        return self.get_db().execute(sql, params)

    def result(self):
        """Result"""
        if self._action in ('select', 'count', ):
            return self
        return self.execute()

    def get_db(self):
        return get_db(self.using)

    def begin(self):
        return self.get_db().begin()

    def commit(self):
        return self.get_db().commit()

    def rollback(self):
        return self.get_db().rollback()

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
        if name[0] == '_':
            raise AttributeError
        parts = name.split(smartsql.LOOKUP_SEP, 1)
        result = {'field': parts[0], }
        signals.send_signal(signal='field_conversion', sender=self, result=result, field=parts[0], model=self.model)
        parts[0] = result['field']
        if parts[0] == 'pk':
            parts[0] = self.model._meta.pk
        if isinstance(self.model._meta.relations.get(parts[0], None), ForeignKey):
            parts[0] = self.model._meta.relations.get(parts[0]).field
        return super(Table, self).__getattr__(smartsql.LOOKUP_SEP.join(parts))

    def as_(self, alias):
        return TableAlias(alias, self)


class TableAlias(smartsql.TableAlias, Table):
    """Table alias class"""
    @property
    def model(self):
        return self.table.model


def qn(name, using='default'):
    """Quotes DB name"""
    engine = get_db(using).engine
    return smartsql.qn(name, SMARTSQL_DIALECTS.get(engine, engine))


def cascade(parent, child, parent_rel):
    child.delete()


def set_null(parent, child, parent_rel):
    setattr(child, parent_rel.rel_field, None)
    child.save()


def do_nothing(parent, child, rel):
    pass


class Relation(object):

    def __init__(self, rel_model, rel_field=None, field=None, qs=None, on_delete=cascade):
        self.rel_model_or_name = rel_model
        self._rel_field = rel_field
        self._field = field
        self.qs = qs
        self.on_delete = on_delete

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
            return registry.get(name)
        return self.rel_model_or_name

    def get_qs(self):
        if isinstance(self.qs, collections.Callable):
            return self.qs(self)
        elif self.qs:
            return self.qs.clone()
        else:
            return self.rel_model.s.qs.clone()

    def filter(self, *a, **kw):
        qs = self.get_qs()
        t = self.rel_model.s
        for fn, param in kw.items():
            f = smartsql.Field(fn, t)
            qs = qs.where(f == param)
        return qs


class ForeignKey(Relation):

    def __init__(self, rel_model, rel_field=None, field=None, qs=None, on_delete=cascade, related_name=None):
        self._related_name = related_name
        super(ForeignKey, self).__init__(rel_model, rel_field, field, qs)

    @property
    def field(self):
        return self._field or '{0}_id'.format(self.rel_model._meta.db_table.split("_").pop())

    @property
    def rel_field(self):
        return self._rel_field or self.rel_model._meta.pk

    @property
    def related_name(self):
        return self._related_name or '{0}_set'.format(self.rel_model.__name__.lower())

    def add_to_class(self, model_class, name):
        super(ForeignKey, self).add_to_class(model_class, name)
        self.add_related()

    def add_related(self):
        try:
            rel_model = self.rel_model
        except ModelNotRegistered:
            return

        if self.related_name in rel_model._meta.relations:
            return

        OneToMany(
            self.model, self.field, self.rel_field,
            None, on_delete=self.on_delete
        ).add_to_class(
            rel_model, self.related_name
        )

    def __get__(self, instance, owner):
        if not instance:
            return self.rel_model
        fk_val = getattr(instance, self.field)
        if fk_val is None:
            return None
        return self.filter(**{self.rel_field: fk_val})[0]

    def __set__(self, instance, value):
        if isinstance(value, Model):
            if not isinstance(value, self.rel_model):
                raise Exception(
                    ('Value should be an instance of "{0}" ' +
                    'or primary key of related instance.').format(
                        self.rel_model._meta.name
                    )
                )
            value = value._get_pk()
        setattr(instance, self.field, value)

    def __delete__(self, instance):
        setattr(instance, self.field, None)


class OneToMany(Relation):

    @property
    def rel_field(self):
        return self._rel_field or '{0}_id'.format(self.model._meta.db_table.split("_").pop())

    @property
    def field(self):
        return self._field or self.model._meta.pk

    def __get__(self, instance, owner):
        if not instance:
            return self.rel_model
        return self.filter(**{self.rel_field: getattr(instance, self.field)})


data_registry = DataRegistry()


@data_registry.to_sql('sqlite', Model)
@data_registry.to_sql('mysql', Model)
@data_registry.to_sql('postgres', Model)
def model_to_sql(val):
    return val.pk
