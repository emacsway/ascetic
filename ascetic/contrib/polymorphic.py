# -*- coding: utf-8 -*-
from collections import OrderedDict
from .. import validators
from ..models import IdentityMap, ObjectDoesNotExist, OneToOne, Result, classproperty, model_registry, mapper_registry, to_tuple
from ..utils import cached_property
from .gfk import GenericForeignKey

# TODO: Support for native support inheritance:
# http://www.postgresql.org/docs/9.4/static/tutorial-inheritance.html
# http://www.postgresql.org/docs/9.4/static/ddl-inherit.html


class NativePolymorphicMapper(object):
    pass


class PolymorphicMapper(object):

    @classmethod
    def get_polymorphic_bases(cls, derived_model):
        bases = []
        for base in derived_model.__bases__:
            if base in mapper_registry and getattr(mapper_registry[base], 'polymorphic', False):
                bases.append(mapper_registry[base])
        if not bases:
            for base in derived_model.__bases__:
                bases += cls.get_polymorphic_bases(base)
        return tuple(bases)

    @cached_property
    def polymorphic_bases(self):
        return self.get_polymorphic_bases(self.model)

    @cached_property
    def polymorphic_mro(self):
        pass  # TODO: Fix the diamond inheritance problem???

    @cached_property
    def polymorphic_fields(self):
        fields = OrderedDict()
        for base in self.polymorphic_bases:
            for k, v in self.fields.items():
                fields.update(base.polymorphic_fields)
        for name, field in self.fields.items():
            fields[name] = field
        return fields

    @cached_property
    def polymorphic_columns(self):
        cols = OrderedDict()
        for base in self.polymorphic_bases:
            for k, v in self.fields.items():
                cols.update(base.polymorphic_columns)
        for name, col in self.fields.items():
            cols[name] = col
        return cols

    def _create_query(self):
        bases = self.polymorphic_bases
        if bases:
            base = bases[-1]
            q = base._create_query()
            derived_mappers = (self,) + bases[:-1]
            for derived_mapper in derived_mappers:
                t = derived_mapper.sql_table
                q = q.fields(
                    *self.get_sql_fields()
                ).tables((
                    q.tables() & t
                ).on(
                    t.pk == base.sql_table.pk
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
                # TODO: support multiple inheritance
                setattr(model, pk_rel_name, OneToOne(
                    base,
                    field=mapper_registry[model].pk,
                    rel_field=mapper_registry[base].pk,
                    rel_name=model.__name__.lower(),
                    query=(lambda rel: mapper_registry[rel.rel_model].query.polymorphic(False)),  # TODO: rel.model instead of rel.rel_model?
                    rel_query=(lambda rel: mapper_registry[rel.rel_model].query.polymorphic(False))
                ))
                break
        else:
            if getattr(mapper_registry[model], 'polymorphic', False):
                setattr(model, "concrete_instance", GenericForeignKey(
                    type_field="polymorphic_type_id",
                    rel_field=(lambda rel: mapper_registry[rel.rel_model].pk),
                    field=mapper_registry[model].pk,
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
        identity_map = IdentityMap(self._using)
        key = self._make_identity_key(self.model, tuple(data_mapped[i] for i in to_tuple(self.pk)))
        if identity_map.exists(key):
            try:
                return identity_map.get(key)
            except ObjectDoesNotExist:
                pass
        obj = self._do_load(data_mapped)
        self.set_original_data(obj, data_mapped)
        self.mark_new(obj, False)
        identity_map.add(key, obj)
        return obj

    def validate(self, obj, fields=frozenset(), exclude=frozenset()):
        errors = {}
        for base in self.polymorphic_bases:
            try:
                base.validate(obj, fields=fields, exclude=exclude)
            except validators.ValidationError as e:
                errors.update(e.args[0])

        try:
            super(PolymorphicMapper, self).validate(obj, fields=fields, exclude=exclude)
        except validators.ValidationError as e:
            errors.update(e.args[0])

        if errors:
            raise validators.ValidationError(errors)

    def save(self, obj):
        if not self.polymorphic_fields['polymorphic_type_id'].get_value(obj):
            obj.polymorphic_type_id = mapper_registry[obj.__class__].name
        for base in self.polymorphic_bases:
            new_record = self.is_new(obj)
            base.save(obj)
            for key, base_key in zip(to_tuple(self.pk), to_tuple(base.pk)):
                self.fields[key].set_value(obj, self.polymorphic_fields[base_key].get_value(obj))
            self.mark_new(obj, new_record)
        return super(PolymorphicMapper, self).save(obj)


class PolymorphicResult(Result):

    _polymorphic = True

    def polymorphic(self, val=True):
        self._polymorphic = val
        return self._query

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
            yield obj.concrete_instance if self._polymorphic and hasattr(obj, 'concrete_instance') else obj


def populate_polymorphic(rows):
    if not rows:
        return
    current_model = rows[0].__class__
    current_mapper = mapper_registry[current_model]
    pks = {current_mapper.get_pk(i) for i in rows}
    content_types = {i.polymorphic_type_id for i in rows}
    content_types -= {mapper_registry[current_model].name}
    typical_objects = {}
    for ct in content_types:
        model = model_registry[ct]
        mapper = mapper_registry[model]
        typical_objects[ct] = {mapper.get_pk(i): i for i in mapper.query.where(mapper.sql_table.pk.in_(pks))}
    for i, obj in enumerate(rows):
        if obj.polymorphic_type_id in typical_objects:
            rows[i] = typical_objects[obj.polymorphic_type_id][current_mapper.get_pk(obj)]
