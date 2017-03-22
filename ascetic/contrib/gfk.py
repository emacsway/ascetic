import copy
import operator
import collections
from functools import wraps
from ascetic.mappers import model_registry, mapper_registry, is_model_instance
from ascetic.utils import to_tuple
from ascetic.relations import ForeignKey, OneToMany, cascade
from ascetic.utils import cached_property, SpecialAttrAccessor, SpecialMappingAccessor


class GenericForeignKey(ForeignKey):

    instance = None

    def __init__(self, type_field="object_type_id", related_field=None, field=None, on_delete=cascade, related_name=None, related_query=None):
        self._cache = SpecialMappingAccessor(SpecialAttrAccessor('cache', default=dict))
        self._type_field = type_field
        if not related_field or isinstance(related_field, collections.Callable):
            self._related_field = related_field
        else:
            self._related_field = to_tuple(related_field)
        self._field = field and to_tuple(field)
        self.on_delete = on_delete
        self._related_name = related_name
        self._related_query = related_query

    @cached_property
    def type_field(self):
        return self._type_field

    @cached_property
    def field(self):
        return self._field or ('object_id',)

    @cached_property
    def related_field(self):
        if isinstance(self._related_field, collections.Callable):
            return to_tuple(self._related_field(self))
        return self._related_field

    @cached_property
    def related_model(self):
        return model_registry[getattr(self.instance, self.type_field, None)]

    @cached_property
    def related_query(self):
        if isinstance(self._related_query, collections.Callable):
            return self._related_query(self)
        else:
            return self.related_mapper.query

    def setup_reverse_relation(self):
        pass

    def bind_instance(self, instance):
        c = copy.copy(self)
        c.instance = instance
        return c

    def _bindable(func):
        @wraps(func)
        def _deco(self, instance, *a, **kw):
            return func(self.bind_instance(instance), instance, *a, **kw)
        return _deco

    @_bindable
    def get(self, instance):
        return super(GenericForeignKey, self).get(instance)

    @_bindable
    def set(self, instance, value):
        if is_model_instance(value):
            setattr(instance, self.type_field, mapper_registry[value.__class__].name)
        super(GenericForeignKey, self).set(instance, value)

    @_bindable
    def delete(self, instance):
        setattr(instance, self.type_field(instance.__class__), None)
        super(GenericForeignKey, self).delete(instance)

    _bindable = staticmethod(_bindable)


class GenericRelation(OneToMany):

    @cached_property
    def field(self):
        return self.related_relation.related_field

    @cached_property
    def related_field(self):
        return self.related_relation.field

    @cached_property
    def related_type_field(self):
        return self.related_relation.type_field

    def get(self, instance):
        val = self.get_value(instance)
        cached_query = self._get_cache(instance, self.name)
        # Be sure that value of related fields equals to value of field
        if cached_query is not None and cached_query._cache is not None:
            for cached_obj in cached_query._cache:
                if (self.get_related_value(cached_obj) != val or
                        getattr(cached_obj, self.related_type_field) != mapper_registry[instance.__class__].name):
                    cached_query = None
                    break
        if cached_query is None:
            t = self.related_mapper.sql_table
            q = super(GenericRelation, self).get(instance)
            q = q.where(t.get_field(self.related_type_field) == mapper_registry[instance.__class__].name)
            self._set_cache(instance, self.name, q)
        return self._get_cache(instance, self.name)

    def set(self, instance, object_list):
        val = self.get_value(instance)
        for cached_obj in object_list:
            if is_model_instance(cached_obj):
                self.validate_related_obj(cached_obj)
                if (self.get_related_value(cached_obj) != val or
                        getattr(cached_obj, self.related_type_field) != mapper_registry[instance.__class__].name):
                    return
        self.get(instance)._cache = object_list
