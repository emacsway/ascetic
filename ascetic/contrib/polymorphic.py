# -*- coding: utf-8 -*-
from collections import OrderedDict
from .. import validators
from ..models import OneToOne, Result, classproperty, model_registry, mapper_registry, to_tuple
from ..utils import cached_property
from .gfk import GenericForeignKey

# Django-way with fields and local_fields???


class PolymorphicMapper(object):

    @cached_property
    def polymorphic_parent(self):
        for parent in self.model.mro():
            if parent is not self.model and parent in mapper_registry and getattr(mapper_registry[parent], 'polymorphic', False):
                return mapper_registry[parent]

    @classproperty
    def polymorphic_root(self):
        for parent in self.model.mro().reverse():
            if parent is not self.model and parent in mapper_registry and getattr(mapper_registry[parent], 'polymorphic', False):
                return mapper_registry[parent]

    @cached_property
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
            q = p._create_query()
            q = q.fields(
                *self.get_sql_fields()
            ).tables((
                q.tables() & t
            ).on(
                t.pk == p.sql_table.pk
            ))
        else:
            q = super(PolymorphicMapper, self)._create_query()
        q.result = PolymorphicResult(self)
        return q

    def _do_prepare_model(self, model):
        for base in model.mro():
            if base is not model and getattr(mapper_registry.get(base), 'polymorphic', False):
                pk_rel_name = "{}_ptr".format(base.__name__.lower())
                # self.pk = "{}_id".format(pk_rel_name)  # Useless, pk read from DB
                setattr(model, pk_rel_name, OneToOne(
                    base,
                    field=mapper_registry[model].pk,
                    rel_field=mapper_registry[base].pk,
                    rel_name=model.__name__.lower(),
                    query=(lambda rel: mapper_registry[rel.rel_model].query.polymorphic(False)),
                    rel_query=(lambda rel: mapper_registry[rel.rel_model].query.polymorphic(False))
                ))
                break
        else:
            if getattr(mapper_registry[model], 'polymorphic', False):
                setattr(model, "real_instance", GenericForeignKey(
                    type_field="polymorphic_type_id",
                    field=mapper_registry[model].pk
                ))
        super(PolymorphicMapper, self)._do_prepare_model(self.model)

    def load(self, data, from_db=True):
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
        self.set_original_data(obj, data_mapped)
        self.mark_new(obj, False)
        return obj

    def validate(self, obj, fields=frozenset(), exclude=frozenset()):
        errors = {}
        if self.polymorphic_parent:
            try:
                self.polymorphic_parent.validate(obj, fields=fields, exclude=exclude)
            except validators.ValidationError as e:
                errors.update(e.args[0])

        try:
            super(PolymorphicMapper, self).validate(obj, fields=fields, exclude=exclude)
        except validators.ValidationError as e:
            errors.update(e.args[0])

        if errors:
            raise validators.ValidationError(errors)

    def save(self, obj):
        if not getattr(obj, 'polymorphic_type_id', None):
            obj.polymorphic_type_id = mapper_registry[obj.__class__].name
        if self.polymorphic_parent:
            new_record = self.is_new(obj)
            self.polymorphic_parent.save(obj)
            for key, parent_key in zip(to_tuple(self.pk), to_tuple(self.polymorphic_parent.pk)):
                setattr(obj, key, getattr(obj, parent_key))
            self.mark_new(obj, new_record)
        return super(PolymorphicMapper, self).save(obj)


class PolymorphicResult(Result):

    _polymorphic = True

    def polymorphic(self, val=True):
        c = self._clone()
        c._polymorphic = val
        return c

    def fill_cache(self):
        if self._cache is not None or not self._polymorphic:
            return super(PolymorphicResult, self).fill_cache()

        if self._cache is None:
            polymorphic, self._polymorphic = self._polymorphic, False
            self._cache = list(self.iterator())
            populate_polymorphic(self._cache)
            self.populate_prefetch()
            self._polymorphic = polymorphic
        return self

    def iterator(self):
        for obj in super(PolymorphicResult, self).iterator():
            yield obj.real_instance if self._polymorphic and hasattr(obj, 'real_instance') else obj

    def _clone(self, *args, **kwargs):
        c = super(PolymorphicResult, self).clone(*args, **kwargs)
        c._polymorphic = self._polymorphic
        return c


def populate_polymorphic(rows):
    if not rows:
        return
    current_model = rows[0].__class__
    pks = {i.pk for i in rows}
    content_types = {i.polymorphic_type_id for i in rows}
    content_types -= set((mapper_registry[current_model].name,))
    typical_objects = {}
    for ct in content_types:
        model = model_registry[ct]
        typical_objects[ct] = {i.pk: i for i in mapper_registry[model].query.where(model.s.pk.in_(pks))}
    for i, obj in enumerate(rows):
        if obj.polymorphic_type_id in typical_objects:
            rows[i] = typical_objects[obj.polymorphic_type_id][obj.pk]
