import operator
import collections
from ..models import ForeignKey, OneToMany, cascade, model_registry, mapper_registry, to_tuple, is_model_instance


class GenericForeignKey(ForeignKey):

    def __init__(self, type_field="object_type_id", rel_field=None, field=None, on_delete=cascade, rel_name=None, rel_query=None):
        self._type_field = type_field
        self._rel_field = rel_field and to_tuple(rel_field)
        self._field = field and to_tuple(field)
        self.on_delete = on_delete
        self._rel_name = rel_name
        self._rel_query = rel_query

    def type_field(self, owner):
        return self._type_field

    def field(self, owner):
        return self._field or ('object_id',)

    def rel_field(self, owner):
        return self._rel_field

    def rel_name(self, owner):
        if self._rel_name is None:
            return '{0}_set'.format(self.model(owner).__name__.lower())
        elif isinstance(self._rel_name, collections.Callable):
            return self._rel_name(self, owner)
        else:
            return self._rel_name

    def rel_model(self, instance):
        if isinstance(instance, type):
            raise TypeError('"instance" argument should be instance of model, not class.')
        return model_registry[getattr(instance, self.type_field(instance.__class__))]

    def rel_query(self, instance):
        if isinstance(instance, type):
            raise TypeError('"instance" argument should be instance of model, not class.')
        if isinstance(self._rel_query, collections.Callable):
            return self._rel_query(self, instance)
        else:
            return mapper_registry[self.rel_model(instance)].query

    def get_rel_where(self, instance):
        owner = instance.__class__
        t = mapper_registry[self.rel_model(instance)].sql_table
        return reduce(operator.and_,
                      ((t.__getattr__(rf) == getattr(instance, f, None))
                       for f, rf in zip(self.field(owner), self.rel_field(owner))))

    @property
    def setup_related(self):
        raise AttributeError

    def __get__(self, instance, owner):
        if not instance:
            return self

        val = self.get_value(owner, instance)
        if not all(val):
            return None

        cached_obj = self._get_cache(instance, self.name(owner))
        rel_field = self.rel_field(owner)
        rel_model = self.rel_model(instance)
        if not isinstance(cached_obj, rel_model) or self.get_rel_value(owner, cached_obj) != val:
            if self._rel_query is None and rel_field == to_tuple(mapper_registry[rel_model].pk):
                obj = rel_model.get(val)  # to use IdentityMap
            else:
                obj = self.rel_query(instance).where(self.get_rel_where(instance))
            self._set_cache(instance, self.name(owner), obj)
        return instance._cache[self.name(owner)]

    def __set__(self, instance, value):
        owner = instance.__class__
        if is_model_instance(value):
            setattr(instance, self.type_field(owner), mapper_registry[value.__class__].name)
            self._set_cache(instance, self.name(owner), value)
            value = self.get_rel_value(owner, value)
        self.set_value(owner, instance, value)

    def __delete__(self, instance):
        owner = instance.__class__
        self._set_cache(instance, self.name(owner), None)
        setattr(instance, self.type_field(instance.__class__), None)
        self.set_value(owner, instance, None)


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
        rel_type_field = self.rel_type_field(owner)
        val = self.get_value(owner, instance)
        cached_query = self._get_cache(instance, self.name(owner))
        # Be sure that value of related fields equals to value of field
        if cached_query is not None and cached_query._cache is not None:
            for cached_obj in cached_query._cache:
                if (self.get_rel_value(owner, cached_obj) != val or
                        getattr(cached_obj, rel_type_field) != mapper_registry[type(instance)].name):
                    cached_query = None
                    break
        if cached_query is None:
            t = mapper_registry[self.rel_model(owner)].sql_table
            q = super(GenericRelation, self).__get__(instance, owner)
            q = q.where(t.__getattr__(rel_type_field) == mapper_registry[instance.__class__].name)
            self._set_cache(instance, self.name(owner), q)
        return self._get_cache(instance, self.name(owner))

    def __set__(self, instance, object_list):
        owner = instance.__class__
        rel_type_field = self.rel_type_field(owner)
        val = self.get_value(owner, instance)
        for cached_obj in object_list:
            if is_model_instance(cached_obj):
                if not isinstance(cached_obj, self.rel_model(owner)):
                    raise Exception('Value should be an instance of "{0}" or primary key of related instance.'.format(
                        mapper_registry[self.rel_model(owner)].name
                    ))
                if (self.get_rel_value(owner, cached_obj) != val or
                        getattr(cached_obj, rel_type_field) != mapper_registry[instance.__class__].name):
                    return
        self.__get__(instance, owner)._cache = object_list
