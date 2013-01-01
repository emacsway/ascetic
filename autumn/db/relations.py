from __future__ import absolute_import, unicode_literals
from .smartsql import RelationQSMixIn, smartsql
from ..models import registry

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

    def set_up(self, instance, owner):
        if isinstance(self.model, string_types):
            self.model = registry.get(self.model)

    def set_field(self, model):
        if self.field is None and model:
            self.field = '{0}_id'.format(model._meta.db_table.split("_").pop())


class ForeignKey(Relation):

    def set_up(self, instance, owner):
        super(ForeignKey, self).set_up(instance, owner)
        self.set_field(self.model)

    def __get__(self, instance, owner):
        self.set_up(instance, owner)
        if not instance:
            return self.model
        return self.filter(**{self.model._meta.pk: getattr(instance, self.field)})[0]


class OneToMany(Relation):

    def set_up(self, instance, owner):
        super(OneToMany, self).set_up(instance, owner)
        self.set_field(instance)

    def __get__(self, instance, owner):
        self.set_up(instance, owner)
        if not instance:
            return self.model
        return self.filter(**{self.field: getattr(instance, instance._meta.pk)})
