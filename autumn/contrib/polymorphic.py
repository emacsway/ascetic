# -*- coding: utf-8 -*-
from .. import models

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
            if getattr(base._meta, 'polymorphic', None):
                pk_rel_name = "{}_ptr".format(base.__name__.lower())
                new_cls.pk = "{}_id".format(pk_rel_name)
                models.OneToOne(
                    base,
                    field=new_cls.pk,
                    to_field=base.pk
                ).add_to_class(
                    new_cls, pk_rel_name
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

    @property
    def root_model(self):
        for cls in type(self).mro().reverse():
            if getattr(cls._meta, 'polimorphic', None):
                return cls

    @models.classproperty
    def parent_model(self):
        for cls in type(self).mro():
            if getattr(cls._meta, 'polimorphic', None):
                return cls

    @property
    def parent_model_instance_(self):
        if self.parent_model:
            obj = getattr(self, "{}_prt".format(
                self.parent_model.__name__.lower()
            ))
            return obj

    def __setattr__(self, name, value):
        """Records when fields have changed"""
        if self.parent_model_instance:
            self.parent_model_instance.__setattr__(name, value)
        super(PolymorphicModel, self).__setattr__(name, value)

    def _set_data(self, *a, **kw):
        if self.parent_model_instance:
            self.parent_model_instance._set_data(*a, **kw)
        super(PolymorphicModel, self)._set_data(*a, **kw)
        return self

    def is_valid(self, *a, **kw):
        if self.parent_model_instance:
            if not self.parent_model_instance.is_valid(*a, **kw):
                parent_valid = False
        self_valid = super(PolymorphicModel, self).is_valid(*a, **kw)
        return parent_valid and self_valid

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
            qs = cls.parent_model.qs
            qs = qs.fields(
                *t.get_fields()
            ).tables((
                qs.tables() & t
            ).on(
                t.pk == cls.parent_model.s.pk
            ))
            t.qs = qs
        return cls._s
