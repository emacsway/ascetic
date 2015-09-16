import operator
import collections
from ..models import ForeignKey, OneToMany, cascade, model_registry, mapper_registry, to_tuple, is_model_instance
from ..utils import cached_property


class GenericForeignKey(ForeignKey):

    def __init__(self, type_field="object_type_id", rel_field=None, field=None, on_delete=cascade, rel_name=None, rel_query=None):
        self._type_field = type_field
        self._rel_field = rel_field and to_tuple(rel_field)
        self._field = field and to_tuple(field)
        self.on_delete = on_delete
        self._rel_name = rel_name
        self._rel_query = rel_query

    @cached_property
    def type_field(self):
        return self._type_field

    @cached_property
    def field(self):
        return self._field or ('object_id',)

    @cached_property
    def rel_field(self):
        return self._rel_field

    @cached_property
    def rel_name(self):
        if self._rel_name is None:
            return '{0}_set'.format(self.model.__name__.lower())
        elif isinstance(self._rel_name, collections.Callable):
            return self._rel_name
        else:
            return self._rel_name

    def rel_model(self, instance):
        return model_registry[getattr(instance, self.type_field)]

    def rel_query(self, instance):
        if isinstance(self._rel_query, collections.Callable):
            return self._rel_query(self, instance)
        else:
            return mapper_registry[self.rel_model(instance)].query

    def get_rel_where(self, instance):
        t = mapper_registry[self.rel_model(instance)].sql_table
        return reduce(operator.and_,
                      ((t.__getattr__(rf) == getattr(instance, f, None))
                       for f, rf in zip(self.field, self.rel_field)))

    @property
    def setup_related(self):
        raise AttributeError

    def get(self, instance):
        val = self.get_value(instance)
        if not all(val):
            return None

        cached_obj = self._get_cache(instance, self.name)
        rel_field = self.rel_field
        rel_model = self.rel_model(instance)
        if not isinstance(cached_obj, rel_model) or self.get_rel_value(cached_obj) != val:
            if self._rel_query is None and rel_field == to_tuple(mapper_registry[rel_model].pk):
                obj = mapper_registry[rel_model].get(val)  # to use IdentityMap
            else:
                obj = self.rel_query(instance).where(self.get_rel_where(instance))
            self._set_cache(instance, self.name, obj)
        return instance._cache[self.name]

    def set(self, instance, value):
        if is_model_instance(value):
            setattr(instance, self.type_field, mapper_registry[value.__class__].name)
            self._set_cache(instance, self.name, value)
            value = self.get_rel_value(value)
        self.set_value(instance, value)

    def delete(self, instance):
        self._set_cache(instance, self.name, None)
        setattr(instance, self.type_field(instance.__class__), None)
        self.set_value(instance, None)


class GenericRelation(OneToMany):

    @cached_property
    def field(self):
        return self.rel.rel_field

    @cached_property
    def rel_field(self):
        return self.rel.field

    @cached_property
    def rel_type_field(self):
        return self.rel.type_field

    def get(self, instance):
        rel_type_field = self.rel_type_field
        val = self.get_value(instance)
        cached_query = self._get_cache(instance, self.name)
        # Be sure that value of related fields equals to value of field
        if cached_query is not None and cached_query._cache is not None:
            for cached_obj in cached_query._cache:
                if (self.get_rel_value(cached_obj) != val or
                        getattr(cached_obj, rel_type_field) != mapper_registry[type(instance)].name):
                    cached_query = None
                    break
        if cached_query is None:
            t = mapper_registry[self.rel_model].sql_table
            q = super(GenericRelation, self).get(instance)
            q = q.where(t.__getattr__(rel_type_field) == mapper_registry[instance.__class__].name)
            self._set_cache(instance, self.name, q)
        return self._get_cache(instance, self.name)

    def set(self, instance, object_list):
        rel_type_field = self.rel_type_field
        val = self.get_value(instance)
        for cached_obj in object_list:
            if is_model_instance(cached_obj):
                self.validate_rel_obj(cached_obj)
                if (self.get_rel_value(cached_obj) != val or
                        getattr(cached_obj, rel_type_field) != mapper_registry[instance.__class__].name):
                    return
        self.get(instance)._cache = object_list
