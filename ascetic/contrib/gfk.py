import copy
from ascetic.interfaces import IBaseRelation
from ascetic.mappers import mapper_registry
from ascetic.relations import ForeignKey, OneToMany, cascade
from ascetic.utils import cached_property, to_tuple


class GenericForeignKey(IBaseRelation):
    mapper_registry = mapper_registry
    descriptor = None
    owner = None

    def __init__(self, type_field="object_type_id", related_field=None, field=None, on_delete=cascade,
                 related_name=None, related_query=None, query=None):
        self._type_field = type_field
        self._related_field = related_field
        self._field = field and to_tuple(field)
        self.on_delete = on_delete
        self._related_name = related_name
        self._related_query = related_query
        self._query = query

    def _make_relation(self, instance):
        related_model = self.get_mapper(getattr(instance, self.type_field)).model
        relation = ForeignKey(
            related_model=related_model,
            related_field=self._related_field,
            field=self.field,
            on_delete=self.on_delete,
            related_name=self._related_name,
            related_query=self._related_query,
            query=None
        )
        relation.descriptor = self.descriptor
        relation = relation.bind(self.owner)
        return relation

    @cached_property
    def field(self):
        return self._field or ('object_id',)

    @cached_property
    def type_field(self):
        return self._type_field

    def setup_reverse_relation(self):
        pass

    def bind(self, owner):
        c = copy.copy(self)
        c.owner = owner
        return c

    def get(self, instance):
        return self._make_relation(instance).get(instance)

    def set(self, instance, value):
        if self.get_mapper(value.__class__):
            setattr(instance, self.type_field, self.get_mapper(value.__class__).name)
        self._make_relation(instance).set(instance, value)

    def delete(self, instance):
        setattr(instance, self.type_field, None)
        self._make_relation(instance).delete(instance)

    def get_mapper(self, model_or_name):
        return self.mapper_registry[model_or_name]


class GenericRelation(OneToMany):

    @cached_property
    def related_field(self):
        return self.related_relation.field

    @cached_property
    def related_type_field(self):
        return self.related_relation.type_field

    def get_related_where(self, obj):
        where = super(GenericRelation, self).get_related_where(obj)
        return where & (self.related_mapper.sql_table.get_field(self.related_type_field) ==
                        self.get_mapper(obj.__class__).name)

    def validate_cached_related_obj(self, obj, cached_related_obj):
        super(GenericRelation, self).validate_cached_related_obj(obj, cached_related_obj)
        if getattr(cached_related_obj, self.related_type_field) != self.get_mapper(obj.__class__).name:
            raise ValueError
