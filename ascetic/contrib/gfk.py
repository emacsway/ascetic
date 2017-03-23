import copy
import collections
from functools import wraps
from ascetic.interfaces import IBaseRelation
from ascetic.mappers import model_registry, mapper_registry, is_model_instance
from ascetic.relations import ForeignKey, OneToMany, cascade
from ascetic.utils import cached_property, to_tuple


class GenericForeignKey(IBaseRelation):
    descriptor = None
    instance = None
    owner = None

    def __init__(self, type_field="object_type_id", related_field=None, field='object_id', on_delete=cascade,
                 related_name=None, related_query=None, query=None):
        self._type_field = type_field

        if not related_field or isinstance(related_field, collections.Callable):
            self._related_field = related_field
        else:
            self._related_field = to_tuple(related_field)

        self._field = field and to_tuple(field)
        self.on_delete = on_delete
        self._related_name = related_name
        self._related_query = related_query
        self._query = query

    def _make_relation(self, instance):
        related_model = model_registry[getattr(instance, self.type_field, None)]
        relation = ForeignKey(
            related_model = related_model,
            related_field=self.related_field,
            field=self.field,
            on_delete=self.on_delete,
            related_name=self._related_name,
            related_query=self._related_query,
            query=None
        )
        relation.descriptor = self.descriptor
        relation = relation.bind(self.owner)
        return relation

    @cached_property
    def field(self):
        return self._field or ('object_id',)

    @cached_property
    def related_field(self):
        if isinstance(self._related_field, collections.Callable):
            return to_tuple(self._related_field(self))
        return self._related_field

    @cached_property
    def type_field(self):
        return self._type_field

    def setup_reverse_relation(self):
        pass

    def bind(self, owner):
        c = copy.copy(self)
        c.owner = owner
        return c

    def get(self, instance):
        return self._make_relation(instance).get(instance)

    def set(self, instance, value):
        if is_model_instance(value):
            setattr(instance, self._type_field, mapper_registry[value.__class__].name)
        self._make_relation(instance).set(instance, value)

    def delete(self, instance):
        setattr(instance, self._type_field, None)
        self._make_relation(instance).delete(instance)


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
