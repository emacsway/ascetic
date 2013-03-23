from __future__ import absolute_import, unicode_literals
import re
import collections
from . import settings
from .connections import get_db
from .smartsql import classproperty, Table, smartsql, qn

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)


class ModelRegistry(object):
    models = {}

    def add(self, model):
        self.models[".".join((model.__module__, model.__name__))] = model

    def get(self, model_name):
        return self.models[model_name]

registry = ModelRegistry()


class ModelOptions(object):
    """Model options"""

    pk = 'id'
    using = 'default'

    def __init__(self, model, **kw):
        """Instance constructor"""
        for k, v in kw:
            setattr(self, k, v)

        self.model = model
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
        q = db.execute(
            'SELECT * FROM {0} LIMIT 1'.format(qn(self.db_table))
        )
        self.fields = [f[0] for f in q.description]
        if hasattr(db, 'describe_table'):
            self.schema = db.describe_table(self.db_table)


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

        registry.add(new_cls)
        settings.send_signal(signal='class_prepared', sender=new_cls, using=new_cls._meta.using)
        return new_cls


class Model(ModelBase(bytes("NewBase"), (object, ), {})):
    """Model class"""

    _ss = None

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

    def is_valid(self):
        """Returns boolean on whether all ``validations`` pass"""
        self._validate()
        return not self._errors

    def _validate(self):
        """Tests all ``validations``"""
        self._errors = {}
        for key, validators in list(getattr(self._meta, 'validations', {}).items()):
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
            self._set_pk(get_db(self._meta.using).last_insert_id(cursor))
        return True

    def _update(self):
        """Uses SQL UPDATE to update record"""
        params = [getattr(self, f) for f in self._changed]
        type(self).qs.where(type(self).ss.pk == self.pk).update(dict(zip(self._changed, params)))

    def delete(self):
        """Deletes record from database"""
        self._send_signal(signal='pre_delete')
        type(self).qs.where(type(self).ss.pk == self.pk).delete()
        self._send_signal(signal='post_delete')
        return True

    def _send_signal(self, *a, **kw):
        """Sends signal"""
        kw.update({
            'sender': type(self),
            'instance': self,
            'using': self._meta.using,
        })
        return settings.send_signal(*a, **kw)

    @classproperty
    def ss(cls):
        if cls._ss is None:
            cls._ss = Table(cls)
        return cls._ss

    @classproperty
    def qs(cls):
        return cls.ss.qs

    @classmethod
    def get(cls, _obj_pk=None, **kwargs):
        'Returns QS object'
        if _obj_pk is not None:
            return cls.get(**{cls._meta.pk: _obj_pk})[0]

        qs = cls.qs
        for k, v in kwargs.items():
            qs = qs.where(smartsql.Field(k, cls.ss) == v)
        return qs

    class ValidationError(Exception):
        pass
