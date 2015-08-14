import collections
from ..models import Model, ForeignKey, OneToMany, cascade, registry, to_tuple, is_model_instance


class GenericForeignKey(ForeignKey):

    def __init__(self, type_field="object_type_id", rel_field=None, field=None, on_delete=cascade, rel_name=None, query=None):
        self._type_field = type_field
        self._rel_field = rel_field and to_tuple(rel_field)
        self._field = field and to_tuple(field)
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
            return '{0}_set'.format(self.model(owner).__name__.lower())
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
            return self.rel_model(instance)._gateway.query

    @property
    def add_related(self):
        raise AttributeError

    def __get__(self, instance, owner):
        if not instance:
            return self
        val = tuple(getattr(instance, f) for f in self.field(owner))

        if not [i for i in val if i is not None]:
            return None

        cached_obj = self._get_cache(instance, self.name(owner))
        rel_field = self.rel_field(owner)
        rel_model = self.rel_model(instance)
        if not isinstance(cached_obj, rel_model) or tuple(getattr(cached_obj, f, None) for f in rel_field) != val:
            if self._query is None and rel_field == to_tuple(rel_model._gateway.pk):
                obj = rel_model.get(val)  # to use IdentityMap
            else:
                t = rel_model._gateway.sql_table
                q = self.query(instance)
                for f, v in zip(rel_field, val):
                    q = q.where(t.__getattr__(f) == v)
                obj = q[0]
            self._set_cache(instance, self.name(owner), obj)
        return instance._cache[self.name(owner)]

    def __set__(self, instance, value):
        owner = instance.__class__
        if is_model_instance(value):
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
        rel_field = self.rel_field(owner)
        rel_type_field = self.rel_type_field(owner)
        val = tuple(getattr(instance, f) for f in self.field(owner))
        cached_query = self._get_cache(instance, self.name(owner))
        # TODO: Be sure that value of related fields equals to value of field
        if cached_query is not None and cached_query._cache is not None:
            for cached_obj in cached_query._cache:
                if (tuple(getattr(cached_obj, f, None) for f in rel_field) != val or
                        getattr(cached_obj, rel_type_field) != type(instance)._gateway.name):
                    cached_query = None
                    break
        if cached_query is None:
            t = self.rel_model(owner)._gateway.sql_table
            q = super(GenericRelation, self).__get__(instance, owner)
            q = q.where(t.__getattr__(rel_type_field) == type(instance)._gateway.name)
            self._set_cache(instance, self.name(owner), q)
        return self._get_cache(instance, self.name(owner))

    def __set__(self, instance, object_list):
        owner = instance.__class__
        rel_field = self.rel_field(owner)
        rel_type_field = self.rel_type_field(owner)
        val = tuple(getattr(instance, f) for f in self.field(owner))
        for cached_obj in object_list:
            if is_model_instance(cached_obj):
                if not isinstance(cached_obj, self.rel_model(owner)):
                    raise Exception(
                        'Value should be an instance of "{0}" or primary key of related instance.'.format(
                            self.rel_model(owner)._gateway.name
                        )
                    )
                if (tuple(getattr(cached_obj, f, None) for f in rel_field) != val or
                        getattr(cached_obj, rel_type_field) != type(instance)._gateway.name):
                    return
        self.__get__(instance, owner)._cache = object_list
