from __future__ import absolute_import, unicode_literals
from .smartsql import RelationQSMixIn, smartsql
from ..models import registry, Model

try:
    str = unicode  # Python 2.* compatible
    str_types = ()
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)


class Relation(RelationQSMixIn):

    def __init__(self, model, field=None, qs=None):
        self.model = model
        self.field = field
        self.qs = qs

    def set_up(self, instance, owner=None):
        if isinstance(self.model, string_types):
            self.model = registry.get(self.model)

    def set_field(self, model):
        if self.field is None and model:
            self.field = '{0}_id'.format(model._meta.db_table.split("_").pop())


class ForeignKey(Relation):

    def set_up(self, instance, owner=None):
        super(ForeignKey, self).set_up(instance, owner)
        self.set_field(self.model)

    def __get__(self, instance, owner):
        self.set_up(instance, owner)
        if not instance:
            return self.model
        fk_val = getattr(instance, self.field)
        if fk_val is None:
            return None
        return self.filter(**{self.model._meta.pk: fk_val})[0]

    def __set__(self, instance, value):
        self.set_up(instance)
        if isinstance(value, Model):
            if not isinstance(value, self.model):
                raise Exception(
                    ('Value should be an instance of "{0}.{1}" ' +
                    'or primary key of related instance.').format(
                        self.model.__module__, self.model.__name__
                    )
                )
            value = value._get_pk()
        setattr(instance, self.field, value)

    def __delete__(self, instance):
        self.set_up(instance)
        setattr(instance, self.field, None)


class OneToMany(Relation):

    def set_up(self, instance, owner=None):
        super(OneToMany, self).set_up(instance, owner)
        self.set_field(instance)

    def __get__(self, instance, owner):
        self.set_up(instance, owner)
        if not instance:
            return self.model
        return self.filter(**{self.field: getattr(instance, instance._meta.pk)})
