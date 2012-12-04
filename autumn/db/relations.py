from __future__ import absolute_import, unicode_literals
from autumn.db.query import Query
from autumn.model import cache

try:
    str = unicode  # Python 2.* compatible
    str_types = ()
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)


class Relation(object):
    
    def __init__(self, model, field=None):            
        self.model = model
        self.field = field
    
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
        conditions = {self.model.Meta.pk: getattr(instance, self.field)}
        return Query(model=self.model, conditions=conditions)[0]

class OneToMany(Relation):
    
    def __get__(self, instance, owner):
        super(OneToMany, self)._set_up(instance, owner)
        if not instance:
            return self.model
        if not self.field:
            self.field = '{0}_id'.format(instance.Meta.table.split("_").pop())
        conditions = {self.field: getattr(instance, instance.Meta.pk)}
        return Query(model=self.model, conditions=conditions)
