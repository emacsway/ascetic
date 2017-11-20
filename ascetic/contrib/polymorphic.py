# -*- coding: utf-8 -*-
from collections import OrderedDict
from ascetic import exceptions
from ascetic.mappers import Load, Mapper, OneToOne, Result
from ascetic.utils import to_tuple
from ascetic.utils import cached_property
from ascetic.contrib.gfk import GenericForeignKey

# TODO: Support for native support inheritance:
# http://www.postgresql.org/docs/9.4/static/tutorial-inheritance.html
# http://www.postgresql.org/docs/9.4/static/ddl-inherit.html


class NativePolymorphicMapper(object):
    pass


class PolymorphicMapper(Mapper):

    result_factory = staticmethod(lambda *a, **kw: PolymorphicResult(*a, **kw))

    def get_polymorphic_bases(self, derived_model):
        bases = []
        for base in derived_model.__bases__:
            if getattr(self.get_mapper(base), 'polymorphic', False):
                bases.append(base)
            else:
                bases += self.get_polymorphic_bases(base)
        return tuple(bases)

    @cached_property
    def polymorphic_bases(self):
        return tuple(self.get_mapper(base_model) for base_model in self.get_polymorphic_bases(self.model))

    # TODO: Fix the diamond inheritance problem???
    # I'm not sure is it a problem... After first base save model will has PK...
    # @cached_property
    # def polymorphic_mro(self):
    #     pass

    @cached_property
    def polymorphic_fields(self):
        fields = OrderedDict()
        for base in self.polymorphic_bases:
            fields.update(base.polymorphic_fields)
        for name, field in self.fields.items():
            fields[name] = field
        return fields

    @cached_property
    def polymorphic_columns(self):
        cols = OrderedDict()
        for base in self.polymorphic_bases:
            cols.update(base.polymorphic_columns)
        for name, col in self.fields.items():
            cols[name] = col
        return cols

    @property
    def query(self):
        bases = self.polymorphic_bases
        if bases:
            base = bases[-1]
            q = base.query
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
            q = super(PolymorphicMapper, self).query
        q.result = PolymorphicResult(self, self._default_db())
        return q

    def _do_prepare_model(self, model):
        for base in model.mro():
            if base is not model and getattr(self.get_mapper(base), 'polymorphic', False):
                pk_related_name = "{}_ptr".format(base.__name__.lower())
                # self.pk = "{}_id".format(pk_related_name)  # Useless, pk read from DB
                # TODO: support multiple inheritance
                setattr(model, pk_related_name, OneToOne(
                    base,
                    field=self.get_mapper(model).pk,
                    related_field=self.get_mapper(base).pk,
                    related_name=model.__name__.lower(),
                    query=(lambda rel: rel.mapper.query.polymorphic(False)),
                    related_query=(lambda rel: rel.related_mapper.query.polymorphic(False))
                ))
                break
        else:
            if getattr(self.get_mapper(model), 'polymorphic', False):
                setattr(model, "concrete_instance", GenericForeignKey(
                    type_field="polymorphic_type_id",
                    related_field=(lambda rel: rel.related_mapper.pk),
                    field=self.get_mapper(model).pk,
                ))
        super(PolymorphicMapper, self)._do_prepare_model(self.model)

    def load(self, data, db, from_db=True, reload=False):
        return PolymorphicLoad(self, data, db, from_db, reload).compute()

    def validate(self, obj, fields=frozenset(), exclude=frozenset()):
        errors = {}
        for base in self.polymorphic_bases:
            try:
                base.validate(obj, fields=fields, exclude=exclude)
            except exceptions.ValidationError as e:
                errors.update(e.args[0])

        try:
            super(PolymorphicMapper, self).validate(obj, fields=fields, exclude=exclude)
        except exceptions.ValidationError as e:
            errors.update(e.args[0])

        if errors:
            raise exceptions.ValidationError(errors)

    def save(self, obj):
        if not self.polymorphic_fields['polymorphic_type_id'].get_value(obj):
            obj.polymorphic_type_id = self.get_mapper(obj.__class__).name
        for base in self.polymorphic_bases:
            new_record = self.is_new(obj)
            base.save(obj)
            for key, base_key in zip(to_tuple(self.pk), to_tuple(base.pk)):
                self.fields[key].set_value(obj, self.polymorphic_fields[base_key].get_value(obj))
            self.is_new(obj, new_record)
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
            self._cache = PopulatePolymorphic(self._cache, self.mapper.get_mapper).compute()
            self.populate_prefetch()
            self._polymorphic = polymorphic
        return self

    def iterator(self):
        for obj in super(PolymorphicResult, self).iterator():
            yield obj.concrete_instance if self._polymorphic and hasattr(obj, 'concrete_instance') else obj


class PopulatePolymorphic(object):

    def __init__(self, rows, mapper_accessor):
        self._rows = rows
        self._get_mapper = mapper_accessor

    def compute(self):
        if not self._rows:
            return []
        return self._get_populated_rows()

    def _get_populated_rows(self):
        rows = self._rows[:]
        typed_objects = self._get_typed_objects()
        for i, obj in enumerate(rows):
            if obj.polymorphic_type_id in typed_objects:
                rows[i] = typed_objects[obj.polymorphic_type_id][self._get_current_mapper().get_pk(obj)]
        return rows

    def _get_typed_objects(self):
        typed_objects = {}
        pks = {self._get_current_mapper().get_pk(i) for i in self._rows}
        for ct in self._get_content_types():
            mapper = self._get_mapper(ct)
            typed_objects[ct] = {mapper.get_pk(i): i for i in mapper.query.where(mapper.sql_table.pk.in_(pks))}
        return typed_objects

    def _get_current_mapper(self):
        current_model = self._rows[0].__class__
        return self._get_mapper(current_model)

    def _get_content_types(self):
        content_types = {i.polymorphic_type_id for i in self._rows}
        content_types -= {self._get_current_mapper().name}
        return content_types


class PolymorphicLoad(Load):
    def _map_data_from_db(self, data, columns=None):
        columns = columns or self._mapper.polymorphic_columns
        return super(PolymorphicLoad, self)._map_data_from_db(data, columns)
