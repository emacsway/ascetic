# -*- coding: utf-8 -*-
from .. import models
from . import gfk

# Not tested yet!!!

# TODO: multilingual based on polymorphic???
# Django-way with fields and local_fields???


class PolymorphicModelBase(models.ModelBase):
    """Metaclass for Model"""

    def __new__(cls, name, bases, attrs):
        new_cls = super(PolymorphicModel, cls).__new__(cls, name, bases, attrs)

        if not hasattr(new_cls, '_meta'):
            return new_cls

        for base in new_cls.__bases__:
            if getattr(base._meta, 'polymorphic', False):
                pk_rel_name = "{}_ptr".format(base.__name__.lower())
                new_cls.pk = "{}_id".format(pk_rel_name)
                models.OneToOne(
                    base,
                    field=new_cls.pk,
                    to_field=base.pk,
                    rel_name=new_cls.__name__.lower(),
                    qs=lambda rel: rel.rel_model.s.qs.polymorphic(False)
                ).add_to_class(
                    new_cls, pk_rel_name
                )
                break
        else:
            if getattr(new_cls._meta, 'polymorphic', False) and not new_cls.root_model:
                gfk.GenericForeignKey(
                    type_field="polymorphic_type_id",
                    field=new_cls.pk
                ).add_to_class(
                    new_cls, "real_model_instance"
                )
        return new_cls


class PolymorphicModel(PolymorphicModelBase(b"NewBase", (models.Model, ), {})):

    def __init__(self, *a, **kw):
        if self.parent_model:
            self.parent_model_instance = self.parent_model()
        else:
            self.parent_model_instance = None
        super(PolymorphicModel, self).__init__(*a, **kw)

    class Meta:
        abstract = True

    @models.classproperty
    def root_model(cls):
        for parent in cls.mro().reverse():
            if parent is not cls and hasattr(parent, '_meta') and getattr(parent._meta, 'polymorphic', False):
                return parent

    @models.classproperty
    def parent_model(cls):
        for parent in cls.mro():
            if parent is not cls and hasattr(parent, '_meta') and getattr(parent._meta, 'polymorphic', False):
                return parent

    @property
    def root_model_instance(self):
        obj = self
        while obj.parent_model_instance:
            obj = obj.parent_model_instance
        if obj != self:
            return obj

    @property
    def parent_model_instance_(self):
        if self.parent_model:
            obj = getattr(self, "{}_prt".format(
                self.parent_model.__name__.lower()
            ))
            return obj

    def __setattr__(self, name, value):
        if name not in type(self)._meta.fields and self.parent_model_instance:
            self.parent_model_instance.__setattr__(name, value)
        else:
            super(PolymorphicModel, self).__setattr__(name, value)

    def __getattr__(self, name):
        if name not in type(self)._meta.fields and self.parent_model_instance:
            self.parent_model_instance.__getattr__(name)
        else:
            super(PolymorphicModel, self).__getattr__(name)

    def _set_data(self, data):
        super(PolymorphicModel, self)._set_data(data)
        for column in self._meta.columns:
            data.pop(column, None)
        if self.parent_model_instance:
            self.parent_model_instance._set_data(data)
        return self

    def is_valid(self, *a, **kw):
        valid = super(PolymorphicModel, self).is_valid(*a, **kw)
        if self.parent_model_instance:
            valid = valid and self.parent_model_instance.is_valid(*a, **kw)
        return valid

    def _validate(self, *a, **kw):
        super(PolymorphicModel, self)._validate(*a, **kw)
        if self.parent_model_instance:
            self.parent_model_instance._validate(*a, **kw)
            self._errors.update(self.parent_model_instance._errors)

    def save(self, *a, **kw):
        if self.parent_model_instance:
            self.parent_model_instance.save(*a, **kw)
        if not self.pk:
            self.pk = self.parent_model_instance.pk
        if not self.polymorphic_type_id:
            self.polymorphic_type_id = type(self)._meta.name
        return super(PolymorphicModel, self).save(*a, **kw)

    def delete(self, *a, **kw):
        result = super(PolymorphicModel, self).delete(*a, **kw)
        if self.parent_model_instance:
            self.parent_model_instance.delete(*a, **kw)
        return result

    def serialize(self, *a, **kw):
        result = {}
        if self.parent_model_instance:
            result.update(self.parent_model_instance.serialize(*a, **kw))
        result.update(super(PolymorphicModel, self).serialize(*a, **kw))
        return result

    @models.classproperty
    def s(cls):
        if '_s' not in cls.__dict__:
            cls._s = t = models.Table(cls)
            if cls.parent_model:
                qs = cls.parent_model.qs
                qs = qs.fields(
                    *t.get_fields()
                ).tables((
                    qs.tables() & t
                ).on(
                    t.pk == cls.parent_model.s.pk
                )).polymorphic(False)
                t.qs = qs
            else:
                t.qs = PolymorphicQuerySet(t).fields(t.get_fields())
        return cls._s


class PolymorphicQuerySet(models.QS):
    """Custom QuerySet for real instances."""

    _polymorphic = True

    def polymorphic(self, val=True):
        c = self._clone()
        c._polymorphic = val
        return c

    def fill_cache(self):
        if self._cache is not None or not self._polymorphic:
            return super(PolymorphicQuerySet, self).fill_cache()

        if self._cache is None:
            polymorphic, self._polymorphic = self._polymorphic, False
            self._cache = list(self.iterator())
            populate_polymorphic(self._cache)
            self.populate_prefetch()
            self._polymorphic = polymorphic
        return self

    def iterator(self):
        for obj in super(PolymorphicQuerySet, self).iterator():
            yield obj.real_model_instance if self._polymorphic and hasattr(obj, 'real_model_instance') else obj

    def _clone(self, *args, **kwargs):
        c = super(PolymorphicQuerySet, self)._clone(*args, **kwargs)
        c._polymorphic = self._polymorphic
        return c


def populate_polymorphic(rows):
    if not rows:
        return
    pks = {i.pk for i in rows}
    content_types = {i.polymorphic_type_id for i in rows}
    content_types -= set((type(rows[0]),))
    typical_objects = {}
    for ct in content_types:
        model = models.registry[ct]
        if model.root_model:  # TODO: remove this condition?
            typical_objects[ct] = {i.pk: i for i in model.qs.where(model.s.pk.in_(pks))}
    for i, obj in enumerate(rows):
        if obj.polymorphic_type_id in typical_objects:
            rows[i] = typical_objects[obj.polymorphic_type_id][obj.pk]
