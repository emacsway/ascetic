from __future__ import absolute_import, unicode_literals
from autumn.db.query import Query
from autumn.models import cache

try:
    str = unicode  # Python 2.* compatible
    str_types = ()
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)


class RelationQSMixIn(object):

    def get_qs(self):
        return self.qs and self.qs.clone() or Query(model=self.model)

    def filter(self, *a, **kw):
        return self.get_qs().filter(*a, **kw)


class Relation(RelationQSMixIn):
    
    def __init__(self, model, field=None, qs=None):            
        self.model = model
        self.field = field
        self.qs = qs
    
    def _set_up(self, instance, owner):
        if isinstance(self.model, string_types):
            self.model = cache.get(self.model)


class ForeignKey(Relation):
        
    def __get__(self, instance, owner):
        super(ForeignKey, self)._set_up(instance, owner)
        if not instance:
            return self.model
        if not self.field:
            self.field = '{0}_id'.format(self.model.Meta.table.split("_").pop())
        return self.filter(**{self.model.Meta.pk: getattr(instance, self.field)})[0]


class OneToMany(Relation):
    
    def __get__(self, instance, owner):
        super(OneToMany, self)._set_up(instance, owner)
        if not instance:
            return self.model
        if not self.field:
            self.field = '{0}_id'.format(instance.Meta.table.split("_").pop())
        return self.filter(**{self.field: getattr(instance, instance.Meta.pk)})
