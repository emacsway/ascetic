# -*- coding: utf-8 -*-
import copy
from collections import OrderedDict
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

    @models.classproperty
    def polymorphic_root(self):
        for parent in self.model.mro().reverse():
            if parent is not self.model and hasattr(parent, '_gateway') and getattr(parent._gateway, 'polymorphic', False):
                return parent

    @models.cached_property
    def polymorphic_columns(self):
        if self.polymorphic_parent:
            cols = self.polymorphic_parent.polymorphic_columns
        else:
            cols = OrderedDict()
        for k, v in self.columns.items():
            cols[k] = v
        return cols

    def _create_query(self):
        p = self.polymorphic_parent
        if p:
            t = self.sql_table
            q = self.parent_model._create_query()
            q = q.fields(
                *self.get_fields()
            ).tables((
                q.tables() & t
            ).on(
                t.pk == p.sql_table.pk
            )).polymorphic(False)
        else:
            q = super(PolymorphicGateway, self)._create_query()
        return q

    def _do_prepare_model(self, model):
        for base in model.__bases__:
            if getattr(base._gateway, 'polymorphic', False):
                pk_rel_name = "{}_ptr".format(base.__name__.lower())
                model.pk = "{}_id".format(pk_rel_name)
                setattr(model, pk_rel_name, models.OneToOne(
                    base,
                    field=model.pk,
                    to_field=base.pk,
                    rel_name=model.__name__.lower(),
                    qs=lambda rel: rel.rel_model.s.q.polymorphic(False)
                ))
                break
        else:
            if getattr(model._gateway, 'polymorphic', False) and not model.root_model:
                gfk.GenericForeignKey(
                    type_field="polymorphic_type_id",
                    field=model.pk
                ).add_to_class(
                    model, "real_instance"
                )

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
        if not obj.polymorphic_type_id:
            obj.polymorphic_type_id = type(self)._gateway.name
        if self.polymorphic_parent:
            # TODO: _base_save()
            self.polymorphic_parent.save(copy.copy(obj))
        return super(PolymorphicGateway, self).save(obj)


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
