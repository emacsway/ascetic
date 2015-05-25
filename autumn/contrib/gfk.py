from ..models import ForeignKey, OneToMany, cascade, registry

# Under construction!!! Not testet yet!!!


class GenericForeignKey(ForeignKey):

    def __init__(self, type_field="object_type_id", rel_field=None, field=None, on_delete=cascade, rel_name=None):
        self._type_field = type_field
        self._rel_field = rel_field
        self._field = field
        self.on_delete = on_delete
        self._rel_name = rel_name

    def type_field(self, owner):
        return self._type_field

    def field(self, owner):
        return self._field or ('object_id',)

    @property
    def rel_model(self):
        raise AttributeError

    @property
    def add_related(self):
        raise AttributeError

    def __get__(self, instance, owner):
        if not instance:
            return self
        type_val = getattr(instance, self.type_field(owner))
        rel_model = registry[type_val]
        val = tuple(getattr(instance, f) for f in self.field(owner))

        if not [i for i in val if i is not None]:
            return None

        cached_obj = self._get_cache(instance, self.name(owner))
        rel_field = self.rel_field(owner)
        if not isinstance(cached_obj, rel_model) or tuple(getattr(cached_obj, f, None) for f in rel_field) != val:
            t = rel_model._gateway.sql_table
            q = rel_model._gateway.base_query
            for f, v in zip(rel_field, val):
                q = q.where(t.__getattr__(f) == v)
            q = self._do_query(q)
            self._set_cache(instance, self.name(owner), q[0])
        return instance._cache[self.name(owner)]

    def __set__(self, instance, value):
        setattr(instance, self.type_field(instance.__class__), type(value)._meta.name)
        super(GenericForeignKey, self).__set__(instance, value)

    def __delete__(self, instance):
        setattr(instance, self.type_field(instance.__class__), None)
        super(GenericForeignKey, self).__delete__(instance)


class GenericRelation(OneToMany):

    def rel_type_field(self, owner):
        rel_model = self.rel_model(owner)
        return getattr(rel_model, self.rel_name(owner)).type_field(rel_model)

    def __get__(self, instance, owner):
        if not instance:
            return self
        q = super(GenericRelation, self).__get__(instance, owner)
        t = self.rel_model(owner)._gateway.sql_table
        return q.where(t.__getattr__(self.rel_type_field(owner)) == type(instance)._gateway.name)
