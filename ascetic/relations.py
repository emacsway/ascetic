import copy
import weakref
import collections
from ascetic.interfaces import IRelation, IRelationDescriptor
from ascetic.mappers import mapper_registry, Mapper
from ascetic.utils import to_tuple
from ascetic.exceptions import MapperNotRegistered
from ascetic.utils import cached_property, SpecialAttrAccessor, SpecialMappingAccessor

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)


def cascade(parent, child, parent_relation, db, visited):
    mapper_registry[child.__class__].delete(child, db=db, visited=visited)


def set_null(parent, child, parent_relation, db, visited):
    setattr(child, parent_relation.related_field, None)
    mapper_registry[child.__class__].save(child, db=db)


def do_nothing(parent, child, parent_rel, db, visited):
    pass

# TODO: descriptor for FileField? Or custom postgresql data type? See http://www.postgresql.org/docs/8.4/static/sql-createtype.html


class RelationDescriptor(IRelationDescriptor):

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


class BaseRelation(object):

    mapper_registry = mapper_registry
    owner = None
    _related_model_or_name = None
    descriptor = NotImplemented

    @cached_property
    def _descriptor_class(self):
        for cls in self.owner.__mro__:
            for name, attr in cls.__dict__.items():
                if attr is self.descriptor():
                    return cls
        raise Exception("Can't find descriptor class for {} in {}.".format(self.owner, self.owner.__mro__))

    @cached_property
    def _descriptor_object(self):
        for cls in self.owner.__mro__:
            for name, attr in cls.__dict__.items():
                if attr is self.descriptor():
                    return getattr(cls, name)
        raise Exception("Can't find descriptor object")

    @cached_property
    def _polymorphic_class(self):
        result_cls = self._descriptor_class
        mro_reversed = list(reversed(self.owner.__mro__))
        mro_reversed = mro_reversed[mro_reversed.index(result_cls) + 1:]
        for cls in mro_reversed:
            if getattr(self.get_mapper(cls), 'polymorphic', False):  # Don't look into mapper.__class__.__dict__, see ModelBase.__new__()
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
        return self._polymorphic_class

    @cached_property
    def related_model(self):
        if isinstance(self._related_model_or_name, string_types):
            name = self._related_model_or_name
            if name == 'self':
                name = self.mapper.name
            return self.get_mapper(name).model
        return self._related_model_or_name

    @cached_property
    def mapper(self):
        return self.get_mapper(self.model)

    @cached_property
    def related_mapper(self):
        return self.get_mapper(self.related_model)

    def get_mapper(self, model_or_name):
        return self.mapper_registry[model_or_name]

    def bind(self, owner):
        c = copy.copy(self)
        c.owner = owner
        return c


class Relation(BaseRelation, IRelation):
    _cache = SpecialMappingAccessor(SpecialAttrAccessor('cache', default=dict))

    def __init__(self, related_model, related_field=None, field=None, on_delete=cascade, related_name=None,
                 related_query=None, query=None):
        if isinstance(related_model, Mapper):
            related_model = related_model.model
        self._related_model_or_name = related_model
        if not related_field or isinstance(related_field, collections.Callable):
            self._related_field = related_field
        else:
            self._related_field = to_tuple(related_field)
        self._field = field and to_tuple(field)
        self.on_delete = on_delete
        self._related_name = related_name
        self._query = query
        self._related_query = related_query

    @cached_property
    def related_relation(self):
        return getattr(self.related_model, self.related_name).relation

    @property
    def query(self):
        if isinstance(self._query, collections.Callable):
            return self._query(self)
        else:
            return self.mapper.query

    @property
    def related_query(self):
        if isinstance(self._related_query, collections.Callable):
            return self._related_query(self)
        else:
            return self.related_mapper.query

    def get_where(self, related_obj):
        t = self.mapper.sql_table
        return t.get_field(self.name) == self.get_related_value(related_obj)  # CompositeExpr is used here

    def get_related_where(self, obj):
        t = self.related_mapper.sql_table
        # Avoid to use self.related_name here. Relation can be unidirectional.
        return t.get_field(self.related_field) == self.get_value(obj)  # CompositeExpr is used here

    def get_join_where(self, prefix=None, related_prefix=None):
        if prefix is None:
            prefix = self.mapper.sql_table
        if related_prefix is None:
            related_prefix = self.related_mapper.sql_table
        # Avoid to use self.related_name here. Relation can be unidirectional.
        return prefix.get_field(self.field) == related_prefix.get_field(self.related_field)  # CompositeExpr is used here

    def get_value(self, obj):
        return tuple(getattr(obj, f, None) for f in self.field)

    def get_related_value(self, related_obj):
        return tuple(getattr(related_obj, f, None) for f in self.related_field)

    def set_value(self, obj, value):
        field = self.field
        if value is None:
            value = (None,) * len(field)
        for f, v in zip(field, to_tuple(value)):
            setattr(obj, f, v)

    def set_related_value(self, related_obj, value):
        related_field = self.related_field
        if value is None:
            value = (None,) * len(related_field)
        for f, v in zip(related_field, to_tuple(value)):
            setattr(related_obj, f, v)

    def validate_related_obj(self, related_obj):
        if not isinstance(related_obj, self.related_model):
            raise ValueError('Object should be an instance of "{0!r}", not "{1!r}".'.format(
                self.related_mapper, type(related_obj)
            ))

    def validate_cached_related_obj(self, obj, cached_related_obj):
        if not isinstance(cached_related_obj, self.related_model):
            raise ValueError
        if self.get_value(obj) != self.get_related_value(cached_related_obj):
            raise ValueError

    def _get_cache(self, instance, key):
        try:
            return self._cache.get(instance)[key]
        except (AttributeError, KeyError):
            return None

    def _set_cache(self, instance, key, value):
        self._cache.update(instance, {key: value})

    def setup_reverse_relation(self):
        try:
            related_model = self.related_model
        except MapperNotRegistered:
            return False

        # add support for templates for related_name.
        # support copy.

        if self.related_name in self.related_mapper.relations:
            return False

        setattr(related_model, self.related_name, RelationDescriptor(self._create_reverse_relation()))
        return True

    def _create_reverse_relation(self):
        raise NotImplementedError


class ForeignKey(Relation):

    @cached_property
    def field(self):
        return self._field or ('{0}_id'.format(self.related_model.__name__.lower()),)

    @cached_property
    def related_field(self):
        if isinstance(self._related_field, collections.Callable):
            return to_tuple(self._related_field(self))
        return self._related_field or to_tuple(self.related_mapper.pk)

    @cached_property
    def related_name(self):
        if self._related_name is None:
            return '{0}_set'.format(self.model.__name__.lower())
        elif isinstance(self._related_name, collections.Callable):
            return self._related_name(self)
        else:
            return self._related_name

    def _create_reverse_relation(self):
        return OneToMany(
            self.owner, self.field, self.related_field,
            on_delete=self.on_delete, related_name=self.name,
            related_query=(lambda rel: self.query)
        )

    def get(self, instance):
        # We MUST use the same connection session which was used for instance accessing
        val = self.get_value(instance)
        if not all(val):
            return None

        cached_obj = self._get_cache(instance, self.name)
        if cached_obj is None or not isinstance(cached_obj, self.related_model) or self.get_related_value(cached_obj) != val:
            db = self.mapper.used_db(instance)
            if self._related_query is None and self.related_field == to_tuple(self.related_mapper.pk):
                obj = self.related_mapper.get(val, db)  # to use IdentityMap before query
            else:
                obj = self.related_query.where(self.get_related_where(instance)).db(db)[0]
            self._set_cache(instance, self.name, obj)
        return self._get_cache(instance, self.name)

    def set(self, instance, value):
        if isinstance(value, self.related_model):
            self.validate_related_obj(value)
            self._set_cache(instance, self.name, value)
            value = self.get_related_value(value)
        self.set_value(instance, value)

    def delete(self, instance):
        self._set_cache(instance, self.name, None)
        self.set_value(instance, None)


class OneToOne(ForeignKey):

    def _create_reverse_relation(self):
        return OneToOne(
            self.owner, self.field, self.related_field,
            on_delete=self.on_delete, related_name=self.name,
            related_query=(lambda rel: self.query)
        )

    def setup_reverse_relation__(self):
        status = super(OneToOne, self).setup_reverse_relation()
        if status:
            # self.on_delete = do_nothing
            # Is "visited" parameter of Mapper.delete() is enough?
            pass
        return status


class OneToMany(Relation):

    @cached_property
    def field(self):
        return self._field or to_tuple(self.mapper.pk)

    @cached_property
    def related_field(self):
        if isinstance(self._related_field, collections.Callable):
            return to_tuple(self._related_field(self))
        return self._related_field or ('{0}_id'.format(self.model.__name__.lower()),)

    @cached_property
    def related_name(self):
        return self._related_name or self.model.__name__.lower()

    def setup_reverse_relation(self):
        # TODO: is it need setup reverse FK?
        return False

    def _create_reverse_relation(self):
        return ForeignKey(
            self.owner, self.field, self.related_field,
            on_delete=self.on_delete, related_name=self.name,
            related_query=(lambda rel: self.query)
        )

    def get(self, instance):
        """
        :type instance: object
        :rtype: sqlbuilder.smartsql.Query
        """
        cached_query = self._get_cache(instance, self.name)
        if cached_query is not None and cached_query._cache is not None:
            for cached_obj in cached_query._cache:
                try:
                    self.validate_cached_related_obj(instance, cached_obj)
                except ValueError:
                    cached_query = None
                    break
        if cached_query is None:
            db = self.mapper.used_db(instance)
            q = self.related_query.where(self.get_related_where(instance)).db(db)
            self._set_cache(instance, self.name, q)
        return self._get_cache(instance, self.name)

    def set(self, instance, object_list):
        for cached_obj in object_list:
            if isinstance(cached_obj, self.related_model):
                self.validate_related_obj(cached_obj)
                try:
                    self.validate_cached_related_obj(instance, cached_obj)
                except ValueError:
                    return
        self.get(instance).result._cache = object_list

    def delete(self, instance):
        self._set_cache(instance, self.name, None)


class ManyToMany(BaseRelation):
    """
    This class it not finished yet!
    """
    def __init__(self, relation, related_model, related_relation, related_name=None):  # associated_model, associated_relation???
        if isinstance(related_model, Mapper):
            related_model = related_model.model
        self._related_model_or_name = related_model
        self._related_relation = related_relation
        self._relation = relation
        self._related_name = related_name

    @cached_property
    def relation(self):
        return getattr(self.model, self._relation)

    @cached_property
    def related_relation(self):
        return getattr(self.related_model, self._related_relation)

    @cached_property
    def field(self):
        return self.relation.field

    @cached_property
    def related_field(self):
        raise self.related_relation.field

    @cached_property
    def related_name(self):
        if self._related_name is None:
            return '{0}_set'.format(self.model.__name__.lower())
        elif isinstance(self._related_name, collections.Callable):
            return self._related_name(self)
        else:
            return self._related_name
