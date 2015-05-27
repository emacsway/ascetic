# -*- coding: utf-8 -*-
import copy
from .. import models, validators
from . import gfk

# Under construction!!! Not testet yet!!!

# TODO: multilingual based on polymorphic???
# Django-way with fields and local_fields???


class PolymorphicGateway(models.Gateway):

    @models.cached_property
    def polymorphic_parent(self):
        for parent in self.model.mro():
            if parent is not self.model and hasattr(parent, '_gateway') and getattr(parent._gateway, 'polymorphic', False):
                return parent._gateway

    @models.cached_property
    def polymorphic_columns(self):
        cols = {}
        g = self
        while g:
            cols.update(g.columns)
            g = g.polymorphic_parent

    def create_instance(self, data, from_db=True):
        if from_db:
            cols = self.polymorphic_columns
            data_mapped = {}
            for key, value in data:
                try:
                    data_mapped[cols[key].name] = value
                except KeyError:
                    data_mapped[key] = value
        else:
            data_mapped = dict(data)
        obj = self.model(**data_mapped)
        obj._original_data = data_mapped
        obj._new_record = False
        return obj

    def validate(self, obj, fields=frozenset(), exclude=frozenset()):
        errors = {}
        try:
            self.polymorphic_parent.validate(self, obj, fields=fields, exclude=exclude)
        except validators.ValidationError as e:
            errors.update(e.args[0])

        try:
            super(PolymorphicGateway, self).validate(self, obj, fields=fields, exclude=exclude)
        except validators.ValidationError as e:
            errors.update(e.args[0])

        if errors:
            raise validators.ValidationError(errors)

    def save(self, obj):
        if not self.polymorphic_type_id:
            self.polymorphic_type_id = type(self)._gateway.name
        if self.polymorphic_parent:
            self.polymorphic_parent.save(copy.copy(obj))
        return super(PolymorphicGateway, self).save(obj)


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
                    qs=lambda rel: rel.rel_model.s.q.polymorphic(False)
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
    def s(cls):
        if '_s' not in cls.__dict__:
            cls._s = t = models.Table(cls)
            if cls.parent_model:
                q = cls.parent_model.q
                q = q.fields(
                    *t.get_fields()
                ).tables((
                    q.tables() & t
                ).on(
                    t.pk == cls.parent_model.s.pk
                )).polymorphic(False)
                t.q = q
            else:
                t.q = PolymorphicQuerySet(t).fields(t.get_fields())
        return cls._s


class PolymorphicQuerySet(models.Q):
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
            typical_objects[ct] = {i.pk: i for i in model.q.where(model.s.pk.in_(pks))}
    for i, obj in enumerate(rows):
        if obj.polymorphic_type_id in typical_objects:
            rows[i] = typical_objects[obj.polymorphic_type_id][obj.pk]
