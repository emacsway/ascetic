import collections
from ..models import Model, ForeignKey, OneToMany, cascade, registry, to_tuple

# Under construction!!! Not testet yet!!!


class GenericForeignKey(ForeignKey):

    def __init__(self, type_field="object_type_id", rel_field=None, field=None, on_delete=cascade, rel_name=None, query=None):
        self._type_field = type_field
        self._rel_field = rel_field
        self._field = field
        self.on_delete = on_delete
        self._rel_name = rel_name
        self._query = query

    def type_field(self, owner):
        return self._type_field

    def field(self, owner):
        return self._field or ('object_id',)

    def rel_field(self, owner):
        return self._rel_field

    def rel_name(self, owner):
        if self._rel_name is None:
            return '{0}_set'.format(owner.__name__.lower())
        elif isinstance(self._rel_name, collections.Callable):
            return self._rel_name(self, owner)
        else:
            return self._rel_name

    def rel_model(self, instance):
        if isinstance(instance, type):
            raise TypeError('"instance" argument should be instance of model, not class.')
        return registry[getattr(instance, self.type_field(instance.__class__))]

    def query(self, instance):
        if isinstance(instance, type):
            raise TypeError('"instance" argument should be instance of model, not class.')
        if isinstance(self._query, collections.Callable):
            return self._query(self, instance)
        else:
            return self.rel_model(instance)._gateway.base_query

    @property
    def add_related(self):
        raise AttributeError

    def __get__(self, instance, owner):
        if not instance:
            return self
        rel_model = self.rel_model(instance)
        val = tuple(getattr(instance, f) for f in self.field(owner))

        if not [i for i in val if i is not None]:
            return None

        cached_obj = self._get_cache(instance, self.name(owner))
        rel_field = self.rel_field(owner)
        if not isinstance(cached_obj, rel_model) or tuple(getattr(cached_obj, f, None) for f in rel_field) != val:
            t = rel_model._gateway.sql_table
            q = self.query(instance)
            for f, v in zip(rel_field, val):
                q = q.where(t.__getattr__(f) == v)
            self._set_cache(instance, self.name(owner), q[0])
        return instance._cache[self.name(owner)]

    def __set__(self, instance, value):
        owner = instance.__class__
        if isinstance(value, Model):
            setattr(instance, self.type_field(owner), value.__class__._gateway.name)
            self._set_cache(instance, self.name(owner), value)
            value = tuple(getattr(value, f) for f in self.rel_field(owner))
        value = to_tuple(value)
        for a, v in zip(self.field(owner), value):
            setattr(instance, a, v)

    def __delete__(self, instance):
        owner = instance.__class__
        self._set_cache(instance, self.name(owner), None)
        setattr(instance, self.type_field(instance.__class__), None)
        for a in self.field(owner):
            setattr(instance, a, None)


class GenericRelation(OneToMany):

    def field(self, owner):
        rel_model = self.rel_model(owner)
        return getattr(rel_model, self.rel_name(owner)).rel_field(rel_model)

    def rel_field(self, owner):
        rel_model = self.rel_model(owner)
        return getattr(rel_model, self.rel_name(owner)).field(rel_model)

    def rel_type_field(self, owner):
        rel_model = self.rel_model(owner)
        return getattr(rel_model, self.rel_name(owner)).type_field(rel_model)

    def __get__(self, instance, owner):
        if not instance:
            return self
        q = super(GenericRelation, self).__get__(instance, owner)
        t = self.rel_model(owner)._gateway.sql_table
        return q.where(t.__getattr__(self.rel_type_field(owner)) == type(instance)._gateway.name)
