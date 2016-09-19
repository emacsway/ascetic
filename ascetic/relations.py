import copy
import weakref
import operator
import collections
from functools import reduce
from ascetic.mappers import mapper_registry, model_registry, is_model_instance, Mapper
from ascetic.utils import to_tuple
from ascetic.exceptions import ModelNotRegistered
from ascetic.utils import cached_property, SpecialAttrAccessor, SpecialMappingAccessor

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)


def cascade(parent, child, parent_rel, using, visited):
    mapper_registry[child.__class__].using(using).delete(child, visited=visited)


def set_null(parent, child, parent_rel, using, visited):
    setattr(child, parent_rel.rel_field, None)
    mapper_registry[child.__class__].using(using).save(child)


def do_nothing(parent, child, parent_rel, using, visited):
    pass

# TODO: descriptor for FileField? Or custom postgresql data type? See http://www.postgresql.org/docs/8.4/static/sql-createtype.html


class BaseRelation(object):

    owner = None
    _rel_model_or_name = None
    descriptor = NotImplemented

    @cached_property
    def descriptor_class(self):
        for cls in self.owner.__mro__:
            for name, attr in cls.__dict__.items():
                if attr is self.descriptor():
                    return cls
        raise Exception("Can't find descriptor class for {} in {}.".format(self.owner, self.owner.__mro__))

    @cached_property
    def descriptor_object(self):
        for cls in self.owner.__mro__:
            for name, attr in cls.__dict__.items():
                if attr is self.descriptor():
                    return getattr(cls, name)
        raise Exception("Can't find descriptor object")

    @cached_property
    def polymorphic_class(self):
        result_cls = self.descriptor_class
        mro_reversed = list(reversed(self.owner.__mro__))
        mro_reversed = mro_reversed[mro_reversed.index(result_cls) + 1:]
        for cls in mro_reversed:
            if getattr(mapper_registry[cls], 'polymorphic', False):  # Don't look into mapper.__class__.__dict__, see ModelBase.__new__()
                break
            result_cls = cls
        return result_cls

    @cached_property
    def name(self):
        self_id = id(self.descriptor())
        for name in dir(self.owner):
            if id(getattr(self.owner, name, None)) == self_id:
                return name

    @cached_property
    def model(self):
        return self.polymorphic_class

    @cached_property
    def rel_model(self):
        if isinstance(self._rel_model_or_name, string_types):
            name = self._rel_model_or_name
            if name == 'self':
                name = mapper_registry[self.model].name
            return model_registry[name]
        return self._rel_model_or_name

    def bind(self, owner):
        c = copy.copy(self)
        c.owner = owner
        return c


class Relation(BaseRelation):

    def __init__(self, rel_model, rel_field=None, field=None, on_delete=cascade, rel_name=None, rel_query=None, query=None):
        self._cache = SpecialMappingAccessor(SpecialAttrAccessor('cache'))
        if isinstance(rel_model, Mapper):
            rel_model = rel_model.model
        self._rel_model_or_name = rel_model
        self._rel_field = rel_field and to_tuple(rel_field)
        self._field = field and to_tuple(field)
        self.on_delete = on_delete
        self._rel_name = rel_name
        self._query = query
        self._rel_query = rel_query

    @cached_property
    def field(self):
        raise NotImplementedError

    @cached_property
    def rel_field(self):
        raise NotImplementedError

    @cached_property
    def rel_name(self):
        raise NotImplementedError

    @cached_property
    def rel(self):
        return getattr(self.rel_model, self.rel_name).relation

    @cached_property
    def query(self):
        if isinstance(self._query, collections.Callable):
            return self._query(self)
        else:
            return mapper_registry[self.model].query

    @cached_property
    def rel_query(self):
        if isinstance(self._rel_query, collections.Callable):
            return self._rel_query(self)
        else:
            return mapper_registry[self.rel_model].query

    def get_where(self, rel_obj):
        t = mapper_registry[self.model].sql_table
        return t.get_field(self.name) == self.get_rel_value(rel_obj)  # Use CompositeExpr
        return reduce(operator.and_,
                      ((t.get_field(f) == rel_val)
                       for f, rel_val in zip(self.field, self.get_rel_value(rel_obj))))

    def get_rel_where(self, obj):
        t = mapper_registry[self.rel_model].sql_table
        # TODO: It's not well to use self.rel_name here. Relation can be non-bidirectional.
        return t.get_field(self.rel_name) == self.get_value(obj)  # Use CompositeExpr
        return reduce(operator.and_,
                      ((t.get_field(rf) == val)
                       for rf, val in zip(self.rel_field, self.get_value(obj))))

    def get_join_where(self):
        t = mapper_registry[self.model].sql_table
        rt = mapper_registry[self.rel_model].sql_table
        return t.get_field(self.name) == rt.get_field(self.rel_name)  # Use CompositeExpr
        return reduce(operator.and_,
                      ((t.get_field(f) == rt.get_field(rf))
                       for f, rf in zip(self.field, self.rel_field)))

    def get_value(self, obj):
        return tuple(getattr(obj, f, None) for f in self.field)

    def get_rel_value(self, rel_obj):
        return tuple(getattr(rel_obj, f, None) for f in self.rel_field)

    def set_value(self, obj, value):
        field = self.field
        if value is None:
            value = (None,) * len(field)
        for f, v in zip(field, to_tuple(value)):
            setattr(obj, f, v)

    def set_rel_value(self, rel_obj, value):
        rel_field = self.rel_field
        if value is None:
            value = (None,) * len(rel_field)
        for f, v in zip(rel_field, to_tuple(value)):
            setattr(rel_obj, f, v)

    def validate_rel_obj(self, rel_obj):
        if not isinstance(rel_obj, self.rel_model):
            raise Exception('Object should be an instance of "{0!r}", not "{1!r}".'.format(
                mapper_registry[self.rel_model], type(rel_obj)
            ))

    def _get_cache(self, instance, key):
        try:
            return self._cache.get(instance)[key]
        except (AttributeError, KeyError):
            return None

    def _set_cache(self, instance, key, value):
        self._cache.update(instance, {key: value})


class ForeignKey(Relation):

    @cached_property
    def field(self):
        return self._field or ('{0}_id'.format(self.rel_model.__name__.lower()),)

    @cached_property
    def rel_field(self):
        return self._rel_field or to_tuple(mapper_registry[self.rel_model].pk)

    @cached_property
    def rel_name(self):
        if self._rel_name is None:
            return '{0}_set'.format(self.model.__name__.lower())
        elif isinstance(self._rel_name, collections.Callable):
            return self._rel_name(self)
        else:
            return self._rel_name

    def setup_related(self):
        try:
            rel_model = self.rel_model
        except ModelNotRegistered:
            return

        if self.rel_name in mapper_registry[rel_model].relations:
            return

        setattr(rel_model, self.rel_name, RelationDescriptor(OneToMany(
            self.owner, self.field, self.rel_field,
            on_delete=self.on_delete, rel_name=self.name,
            rel_query=self._query
        )))

    def get(self, instance):
        val = self.get_value(instance)
        if not all(val):
            return None

        cached_obj = self._get_cache(instance, self.name)
        rel_field = self.rel_field
        rel_model = self.rel_model
        if cached_obj is None or self.get_rel_value(cached_obj) != val:
            if self._rel_query is None and rel_field == to_tuple(mapper_registry[rel_model].pk):
                obj = mapper_registry[rel_model].get(val)  # to use IdentityMap
            else:
                obj = self.rel_query.where(self.get_rel_where(instance))[0]
            self._set_cache(instance, self.name, obj)
        return self._get_cache(instance, self.name)

    def set(self, instance, value):
        if is_model_instance(value):
            self.validate_rel_obj(value)
            self._set_cache(instance, self.name, value)
            value = self.get_rel_value(value)
        self.set_value(instance, value)

    def delete(self, instance):
        self._set_cache(instance, self.name, None)
        self.set_value(instance, None)


class OneToOne(ForeignKey):

    def setup_related(self):
        try:
            rel_model = self.rel_model
        except ModelNotRegistered:
            return

        if self.rel_name in mapper_registry[rel_model].relations:
            return

        setattr(rel_model, self.rel_name, RelationDescriptor(OneToOne(
            self.owner, self.field, self.rel_field,
            on_delete=self.on_delete, rel_name=self.name,
            rel_query=self._query
        )))
        # self.on_delete = do_nothing


class OneToMany(Relation):

    # TODO: is it need setup_related() here to construct related FK?

    @cached_property
    def field(self):
        return self._field or to_tuple(mapper_registry[self.model].pk)

    @cached_property
    def rel_field(self):
        return self._rel_field or ('{0}_id'.format(self.model.__name__.lower()),)

    @cached_property
    def rel_name(self):
        return self._rel_name or self.model.__name__.lower()

    def get(self, instance):
        """
        :type instance: object
        :rtype: sqlbuilder.smartsql.Query
        """
        val = self.get_value(instance)
        cached_query = self._get_cache(instance, self.name)
        # Be sure that value of related fields equals to value of field
        if cached_query is not None and cached_query._cache is not None:
            for cached_obj in cached_query._cache:
                if self.get_rel_value(cached_obj) != val:
                    cached_query = None
                    break
        if cached_query is None:
            q = self.rel_query.where(self.get_rel_where(instance))
            self._set_cache(instance, self.name, q)
        return self._get_cache(instance, self.name)

    def set(self, instance, object_list):
        val = self.get_value(instance)
        for cached_obj in object_list:
            if is_model_instance(cached_obj):
                self.validate_rel_obj(cached_obj)
                if self.get_rel_value(cached_obj) != val:
                    return
        self.get(instance)._cache = object_list


class ManyToMany(BaseRelation):

    def __init__(self, rel_model, rel_relation, relation):  # associated_model, associated_relation???
        if isinstance(rel_model, Mapper):
            rel_model = rel_model.model
        self._rel_model_or_name = rel_model
        self._rel_relation = rel_relation
        self._relation = relation


class RelationDescriptor(object):

    def __init__(self, relation):
        relation.descriptor = weakref.ref(self)
        self.relation = relation
        self._bound_caches = {}

    def get_bound_relation(self, owner):
        try:
            return self._bound_caches[owner]
        except KeyError:
            self._bound_caches[owner] = self.relation.bind(owner)
            return self.get_bound_relation(owner)

    def __get__(self, instance, owner):
        if not instance:
            return self
        return self.get_bound_relation(owner).get(instance)

    def __set__(self, instance, value):
        self.get_bound_relation(instance.__class__).set(instance, value)

    def __delete__(self, instance):
        self.get_bound_relation(instance.__class__).delete(instance)
