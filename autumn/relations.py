from __future__ import absolute_import, unicode_literals
import collections
from sqlbuilder import smartsql
from .models import registry, Model

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)


def cascade(parent, child, parent_rel):
    child.delete()


def set_null(parent, child, parent_rel):
    setattr(child, parent_rel.rel_field, None)
    child.save()


def do_nothing(parent, child, rel):
    pass


class Relation(object):

    def __init__(self, rel_model, rel_field=None, field=None, qs=None):
        self.rel_model_or_name = rel_model
        self._rel_field = rel_field
        self._field = field
        self.qs = qs

    def add_to_class(self, model_class, name):
        self.model = model_class
        self.name = name
        self.model._meta.relations[name] = self
        setattr(self.model, name, self)

    @property
    def rel_model(self):
        if isinstance(self.rel_model_or_name, string_types):
            return registry.get(self.rel_model_or_name)
        return self.rel_model_or_name

    def get_qs(self):
        if isinstance(self.qs, collections.Callable):
            return self.qs(self)
        elif self.qs:
            return self.qs.clone()
        else:
            return self.rel_model.ss.qs.clone()

    def filter(self, *a, **kw):
        qs = self.get_qs()
        t = self.rel_model.ss
        for fn, param in kw.items():
            f = smartsql.Field(fn, t)
            qs = qs.where(f == param)
        return qs


class ForeignKey(Relation):

    @property
    def field(self):
        return self._field or '{0}_id'.format(self.rel_model._meta.db_table.split("_").pop())

    @property
    def rel_field(self):
        return self._rel_field or self.rel_model._meta.pk

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
                    ('Value should be an instance of "{0}.{1}" ' +
                    'or primary key of related instance.').format(
                        self.rel_model.__module__, self.model.__name__
                    )
                )
            value = value._get_pk()
        setattr(instance, self.field, value)

    def __delete__(self, instance):
        setattr(instance, self.field, None)


class OneToMany(Relation):

    def __init__(self, rel_model, rel_field=None, field=None, qs=None, on_delete=cascade):
        self.on_delete = on_delete
        super(OneToMany, self).__init__(rel_model, rel_field, field, qs)

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
