from __future__ import absolute_import

import collections
import copy
import re
from threading import RLock

from sqlbuilder import smartsql

from ascetic import interfaces
from ascetic.exceptions import ObjectDoesNotExist, OrmException, ModelNotRegistered, MapperNotRegistered
from ascetic.fields import Field
from ascetic.utils import to_tuple, SpecialAttrAccessor, SpecialMappingAccessor
from ascetic.databases import databases
from ascetic.signals import pre_save, post_save, pre_delete, post_delete, class_prepared
from ascetic.validators import MappingValidator, CompositeMappingValidator

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)


def is_model_instance(obj):
    return obj.__class__ in model_registry.values()


def is_model(cls):
    return cls in model_registry.values()


def thread_safe(func):
    def _deco(*args, **kwargs):
        with RLock():
            return func(*args, **kwargs)
    return _deco


class BaseRegistry(dict):
    exception_class = OrmException

    def __getitem__(self, key):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            raise self.exception_class("""{} is not registered in {}""".format(key, self.keys()))

    def __call__(self, key):
        return self[key]

    def get(self, key, default=None):
        try:
            return self[key]
        except self.exception_class:
            return default


class ModelRegistry(BaseRegistry):
    exception_class = ModelNotRegistered

    def __getitem__(self, key):
        """
        :type key: str
        :rtype: object
        """
        return BaseRegistry.__getitem__(self, key)

model_registry = get_model = ModelRegistry()


class MapperRegistry(BaseRegistry):
    exception_class = MapperNotRegistered

    def __getitem__(self, key):
        """
        :type key: object
        :rtype: Mapper
        """
        return BaseRegistry.__getitem__(self, key)

mapper_registry = get_mapper = MapperRegistry()


class Mapper(object):

    pk = 'id'
    default_using = 'default'
    _using = 'default'
    abstract = False
    field_factory = Field
    result_factory = staticmethod(lambda *a, **kw: Result(*a, **kw))

    @thread_safe
    def __init__(self, model=None):
        self.original_data = SpecialMappingAccessor(SpecialAttrAccessor('original_data', default=dict))
        self.is_new = SpecialAttrAccessor('new_record', default=True)

        if model:
            self.model = model

        if not hasattr(self, 'name'):
            self.name = self._create_default_name(model)

        model_registry[self.name] = model
        mapper_registry[model] = self
        self.declared_fields = self._create_declared_fields(
            model,
            getattr(self, 'mapping', {}),
            getattr(self, 'defaults', {}),
            getattr(self, 'validations', {}),
            getattr(self, 'declared_fields', {})
        )
        self._inherit(self, (mapper_registry[base] for base in self.model.__bases__ if base in mapper_registry))  # recursive

        if not self.abstract:
            self._using = self.default_using

            if not hasattr(self, 'db_table'):
                self.db_table = self._create_default_db_table(model)

            # fields and columns can be a descriptor for multilingual mapping.
            self.fields = collections.OrderedDict()
            self.columns = collections.OrderedDict()

            for name, field in self.create_fields(databases[self._using].read_fields(self.db_table), self.declared_fields).items():
                self.add_field(name, field)

            self.pk = self._create_pk(self.db_table, self._using, self.columns)
            self.sql_table = self._create_sql_table()

        self._prepare_model(model)
        self._setup_reverse_relations()

    def _create_default_name(self, model):
        return ".".join((model.__module__, model.__name__))

    def _create_default_db_table(self, model):
        return "_".join([
            re.sub(r"[^a-z0-9]", "", i.lower())
            for i in (self.model.__module__.split(".") + [self.model.__name__, ])
        ])

    def _create_declared_fields(self, model, mapping, defaults, validations, declared_fields):
        # We don't need depend on the state of instance, to be able to customise, or even reproduce some steps of initialisation.
        # So, we accept all data as arguments.
        # Dependencies should be made obvious through the use of good routine names, parameter lists,
        # see Chapter 14. Organizing Straight-Line Code of "Code Complete" by Steve McConnell
        # G22: Make Logical Dependencies Physical and
        # G31: Hidden Temporal Couplings of "Clean Code" by Robert Martin
        # TODO: Add class-methods for long methods
        result = {}

        for name in model.__dict__:
            field = getattr(model, name, None)
            if isinstance(field, Field):
                result[name] = field

        for name, column in mapping.items():
            result[name] = self.create_field(name, {'column': column}, declared_fields)

        for name, default in defaults.items():
            result[name] = self.create_field(name, {'default': default}, declared_fields)

        for name, validators in validations.items():
            if not isinstance(validators, (list, tuple)):
                validators = [validators, ]
            result[name] = self.create_field(name, {'validators': validators}, declared_fields)

        return result

    def create_fields(self, field_descriptions, declared_fields):
        fields = collections.OrderedDict()
        reverse_mapping = {field.column: name for name, field in declared_fields.items() if hasattr(field, 'column')}
        for field_description in field_descriptions:
            column = field_description['column']
            name = reverse_mapping.get(column, column)
            fields[name] = self.create_field(name, field_description, declared_fields)
        for name, field in declared_fields.items():
            if name not in fields:
                fields[name] = self.create_field(name, {'virtual': True}, declared_fields)
        return fields

    def create_field(self, name, description, declared_fields=None):
        if declared_fields and name in declared_fields:
            field = copy.deepcopy(declared_fields[name])
            field.__dict__.update(description)
        else:
            field = self.field_factory(**description)
        return field

    def add_field(self, name, field):
        field.name = name
        field.mapper = self
        self.fields[name] = field
        self.columns[field.column] = field

    def _create_pk(self, db_table, using, columns):
        db = databases[using]
        if hasattr(db, 'read_pk'):
            pk = tuple(columns[i].name for i in db.read_pk(db_table))
            return pk[0] if len(pk) == 1 else pk
        return self.__class__.pk

    def _create_sql_table(self):
        return sql.Table(self)

    @property
    def base_query(self):
        """For relations."""
        return sql.Query(
            self.sql_table,
            result=self.result_factory(self)
        ).fields(
            self.get_sql_fields()
        ).using(self.using())

    @property
    def query(self):
        """For selection."""
        return sql.Query(
            self.sql_table,
            result=self.result_factory(self)
        ).fields(
            self.get_sql_fields()
        ).using(self.using())

    def get_sql_fields(self, prefix=None):
        """Returns field list."""
        if prefix is None:
            prefix = self.sql_table
        elif isinstance(prefix, string_types):
            prefix = smartsql.Table(prefix)
        return [prefix.get_field(f.name) for f in self.fields.values() if not getattr(f, 'virtual', False)]

    def _prepare_model(self, model):
        self._do_prepare_model(model)
        PrepareModel(self, model).compute()

    def _do_prepare_model(self, model):
        pass

    def _setup_reverse_relations(self):
        for related_model in model_registry.values():
            for key, rel in mapper_registry[related_model].relations.items():
                try:
                    rel.setup_reverse_relation()
                except ModelNotRegistered:
                    pass

    def _inherit(self, successor, parents):
        for base in parents:  # recursive
            if not base.__dict__.get('polymorphic'):
                for name, field in base.declared_fields.items():
                    if name not in successor.declared_fields:
                        successor.declared_fields[name] = field

    def using(self, alias=False):
        if alias is False:
            return self._using
        if alias is None or alias == self._using:
            return self
        c = copy.copy(self)
        c._using = alias
        return c

    @property
    def relations(self):  # bound_relations(), local_relations() ???
        result = {}
        for model in self.model.__mro__:
            for key, value in model.__dict__.items():
                if isinstance(value, interfaces.IRelationDescriptor):
                    result[key] = value.get_bound_relation(self.model)
        return result

    def load(self, data, from_db=True, reload=False):
        return Load(self, data, from_db, reload).compute()

    def unload(self, obj, fields=frozenset(), exclude=frozenset(), to_db=True):
        return Unload(self, obj, self._get_specified_fields(fields, exclude), to_db).compute()

    def make_identity_key(self, model, pk):
        return (model, to_tuple(pk))

    def get_changed(self, obj):
        if not self.original_data(obj):
            return set(self.fields)
        return set(k for k, v in self.original_data(obj).items() if k in self.fields and self.fields[k].get_value(obj) != v)

    def set_defaults(self, obj):
        for name, field in self.fields.items():
            field.set_default(obj)
        return obj

    def validate(self, obj, fields=frozenset(), exclude=frozenset()):
        # Don't need '__model__' key. Just override this method in subclass.
        self.set_defaults(obj)
        fields = self._get_specified_fields(fields, exclude)
        CompositeMappingValidator(
            MappingValidator({name: self.fields[name].validate for name in fields}),
            self._do_validate
        )(
            {name: self.fields[name].get_value(obj) for name in fields}
        )

    def _do_validate(self, items):
        pass

    def _get_specified_fields(self, fields=frozenset(), exclude=frozenset()):
        if not fields:
            fields = self.fields
        fields = set(fields)  # Can be any iterable type: tuple, list etc.
        fields -= set(exclude)
        fields &= set(self.fields)
        return fields

    def _insert_query(self, obj):
        auto_pk = not all(to_tuple(self.get_pk(obj)))
        data = self.unload(obj, exclude=(to_tuple(self.pk) if auto_pk else ()), to_db=True)
        data = {self.sql_table.get_field(k): v for k, v in data.items()}
        return smartsql.Insert(table=self.sql_table, map=data)

    def _update_query(self, obj):
        data = self.unload(obj, fields=self.get_changed(obj), to_db=True)
        data = {self.sql_table.get_field(k): v for k, v in data.items()}
        return smartsql.Update(table=self.sql_table, map=data, where=(self.sql_table.pk == self.get_pk(obj)))

    def _delete_query(self, obj):
        return smartsql.Delete(table=self.sql_table, where=(self.sql_table.pk == self.get_pk(obj)))

    def save(self, obj):
        """Sets defaults, validates and inserts into or updates database"""
        self.set_defaults(obj)
        self.validate(obj, fields=self.get_changed(obj))
        pre_save.send(sender=self.model, instance=obj, using=self._using)
        is_new = self.is_new(obj)
        result = self._insert(obj) if is_new else self._update(obj)
        post_save.send(sender=self.model, instance=obj, created=is_new, using=self._using)
        self.original_data(obj, **self.unload(obj, to_db=False))
        self.is_new(obj, False)
        return result

    def _insert(self, obj):
        cursor = databases[self._using].execute(self._insert_query(obj))
        if not all(to_tuple(self.get_pk(obj))):
            self.set_pk(obj, self.base_query.result.db.last_insert_id(cursor))
        IdentityMap(self._using).add(self.make_identity_key(self.model, self.get_pk(obj)))

    def _update(self, obj):
        databases[self._using].execute(self._update_query(obj))

    def delete(self, obj, visited=None):
        if visited is None:
            visited = set()
        if self in visited:
            return False
        visited.add(self)
        pre_delete.send(sender=self.model, instance=obj, using=self._using)
        for key, rel in self.relations.items():
            if isinstance(rel, OneToMany):
                for child in getattr(obj, key).iterator():
                    rel.on_delete(obj, child, rel, self._using, visited)
            elif isinstance(rel, OneToOne):
                try:
                    child = getattr(obj, key)
                except ObjectDoesNotExist:
                    pass
                else:
                    rel.on_delete(obj, child, rel, self._using, visited)

        databases[self._using].execute(self._delete_query(obj))
        post_delete.send(sender=self.model, instance=obj, using=self._using)
        IdentityMap(self._using).remove(self.make_identity_key(self.model, self.get_pk(obj)))
        return True

    def get(self, _obj_pk=None, **kwargs):
        if _obj_pk is not None:
            identity_map = IdentityMap(self._using)
            key = self.make_identity_key(self.model, _obj_pk)
            if identity_map.exists(key):
                return identity_map.get(key)
            try:
                obj = self.get(**{k: v for k, v in zip(to_tuple(self.pk), to_tuple(_obj_pk))})
            except ObjectDoesNotExist:
                identity_map.add(key)
                raise
            else:
                # obj added to identity_map by loader (self.load())
                return obj

        if kwargs:
            q = self.query
            for k, v in kwargs.items():
                q = q.where(self.sql_table.get_field(k) == v)
            return q[0]

    def get_pk(self, obj):
        if type(self.pk) == tuple:
            return tuple(self.fields[k].get_value(obj) for k in self.pk)
        return self.fields[self.pk].get_value(obj)

    def set_pk(self, obj, value):
        for k, v in zip(to_tuple(self.pk), to_tuple(value)):
            self.fields[k].set_value(obj, v)


class PrepareModel(object):
    def __init__(self, mapper, model):
        """
        :type mapper: ascetic.mappers.Mapper
        :type model: object
        """
        self._mapper = mapper
        self._model = model

    def compute(self):
        self._clean_model_from_declared_fields()
        self._setup_relations()
        class_prepared.send(sender=self._model, using=self._mapper.using())

    def _clean_model_from_declared_fields(self):
        for name in self._model.__dict__:
            field = getattr(self._model, name, None)
            if isinstance(field, Field):
                delattr(self._model, name)

    def _setup_relations(self):
        if getattr(self._mapper, 'relationships', None):  # TODO: Give me better name (relationships, references, set_relations, ...)
            for key, rel in self._mapper.relationships.items():
                setattr(self._model, key, rel)

        for model in self._model.__mro__:
            for key, value in model.__dict__.items():
                if isinstance(value, interfaces.IBaseRelation):
                    relation_descriptor = RelationDescriptor(value)
                    setattr(model, key, relation_descriptor)


class Load(object):

    def __init__(self, mapper, data, from_db, reload):
        """
        :type mapper: Mapper
        :type data: tuple
        :type from_db: bool
        :type reload: bool
        """
        self._mapper = mapper
        self._data = data
        self._from_db = from_db
        self._reload = reload

    def compute(self):
        if self._from_db:
            data_mapped = self._map_data_from_db(self._data)
        else:
            data_mapped = dict(self._data)
        identity_map = IdentityMap(self._mapper.using())
        key = self._mapper.make_identity_key(self._mapper.model, tuple(data_mapped[i] for i in to_tuple(self._mapper.pk)))
        try:
            obj = identity_map.get(key)
        except KeyError:  # First loading
            obj = self._do_load(data_mapped)
        except ObjectDoesNotExist:  # Serializable transaction level
            raise
        else:
            if reload:
                self._do_reload(obj, data_mapped)
            else:
                return obj
        self._mapper.original_data(obj, data_mapped)
        self._mapper.is_new(obj, False)
        identity_map.add(key, obj)
        return obj

    def _map_data_from_db(self, data, columns=None):
        columns = columns or self._mapper.columns
        data_mapped = {}
        for key, value in data:
            try:
                data_mapped[columns[key].name] = value
            except KeyError:
                data_mapped[key] = value
        return data_mapped

    def _do_load(self, data):
        return self._mapper.model(**data)

    def _do_reload(self, obj, data):
        for name, value in data.items():
            try:
                self._mapper.fields[name].set_value(obj, data[name])
            except KeyError:
                setattr(obj, name, value)


class Unload(object):

    def __init__(self, mapper, obj, fields, to_db):
        """
        :type mapper: Mapper
        :type obj: object
        :type fields: set
        :type to_db: bool
        """
        self._mapper = mapper
        self._obj = obj
        self._fields = fields
        self._to_db = to_db

    def compute(self):
        data = self._do_unload()
        if self._to_db:
            data = self._map_data_to_db(data)
        return data

    def _map_data_to_db(self, data):
        fields = self._mapper.fields
        # check field is not virtual like annotation or subquery.
        return {fields[name].column: value for name, value in data.items()
                if not getattr(fields[name], 'virtual', False)}

    def _do_unload(self):
        return {name: self._mapper.fields[name].get_value(self._obj) for name in self._fields}

from ascetic.relations import RelationDescriptor, OneToOne, OneToMany
from ascetic.query import factory as sql, Result
from ascetic.identity_maps import IdentityMap
