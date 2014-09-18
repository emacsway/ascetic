from .. import models

# Not testet yet!!!


class GenericForeignKey(models.Relation):

    def __init__(self, type_field="type_id", field="object_id"):
        self.type_field = type_field
        self.field = field

    def add_to_class(self, model_class, name):
        self.model = model_class
        self.name = name
        self.model._meta.relations[name] = self
        setattr(self.model, name, self)

    def __get__(self, instance, owner):
        if not instance:
            return self
        fk_val = getattr(instance, self.field)
        type_val = getattr(instance, self.type_field)
        if fk_val is None:
            return None
        cached_obj = instance._cache.get(self.name, None)
        if (cached_obj is not None or cached_obj.pk != fk_val or
                type(cached_obj)._meta.name != type_val):
            rel_model = models.registry[type_val]
            instance._cache[self.name] = rel_model.qs.where(
                rel_model.s.pk == fk_val
            )
        return instance._cache[self.name]

    def __set__(self, instance, value):
        instance._cache[self.name] = value
        setattr(instance, self.field, value._get_pk())
        setattr(instance, self.type_field, type(value)._meta.name)

    def __delete__(self, instance):
        instance._cache.pop(self.name, None)
        setattr(instance, self.field, None)
        setattr(instance, self.type_field, None)


class GenericRelation(models.OneToMany):

    @property
    def rel_name(self):
        if self._rel_name:
            return self._rel_name
        for rel in self.rel_model._meta.relations:
            if rel.rel_model is self.model:
                return rel.name

    @property
    def rel_field(self):
        return getattr(self.rel_model, self.rel_name).field

    @property
    def rel_type_field(self):
        return getattr(self.rel_model, self.rel_name).type_field

    def __get__(self, instance, owner):
        if not instance:
            return self
        return self.filter(**{
            self.rel_field: getattr(instance, self.field),
            self.rel_type_field: type(instance)._meta.name
        })
