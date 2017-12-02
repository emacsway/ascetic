from __future__ import absolute_import

import collections
import copy
import re
from threading import RLock

from sqlbuilder import smartsql

from ascetic import interfaces
from ascetic.exceptions import ObjectDoesNotExist, MapperNotRegistered
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


def thread_safe(func):
    def _deco(*args, **kwargs):
        with RLock():
            return func(*args, **kwargs)
    return _deco


class MapperRegistry(object):

    def __init__(self):
        self._model_registry = dict()
        self._name_registry = dict()

    def register(self, name, model, mapper):
        self._model_registry[model] = mapper
        self._name_registry[name] = mapper

    def __contains__(self, key):
        registry = self._name_registry if isinstance(key, string_types) else self._model_registry
        return key in registry

    def __getitem__(self, model_or_name):
        """
        :type model_or_name: object or str
        :type default: object
        :rtype: Mapper
        """
        registry = self._name_registry if isinstance(model_or_name, string_types) else self._model_registry
        try:
            return registry[model_or_name]
        except KeyError:
            raise MapperNotRegistered("""{} is not registered in {}""".format(model_or_name, registry.keys()))

    def __call__(self, model_or_name):
        return self.__getitem__(model_or_name)

    def values(self):
        return self._model_registry.values()

    def get(self, model_or_name, default=None):
        """
        :type model_or_name: object or str
        :type default: object
        :rtype: Mapper
        """
        try:
            return self[model_or_name]
        except MapperNotRegistered:
            return default

    def clear(self):
        self._model_registry.clear()
        self._name_registry.clear()

mapper_registry = MapperRegistry()


class Mapper(object):

    pk = 'id'
    abstract = False
    mapper_registry = mapper_registry
    original_data = SpecialMappingAccessor(SpecialAttrAccessor('original_data', default=dict))
    is_new = SpecialAttrAccessor('new_record', default=True)
    used_db = SpecialAttrAccessor('db')
    field_factory = Field
    result_factory = staticmethod(lambda *a, **kw: Result(*a, **kw))

    @thread_safe
    def __init__(self, model=None, default_db_accessor=lambda: databases['default']):
        self._default_db = default_db_accessor

        if model:
            self.model = model

        if not hasattr(self, 'name'):
            self.name = self._create_default_name(model)

        self.mapper_registry.register(self.name, model, self)
        self.declared_fields = self._create_declared_fields(
            model,
            getattr(self, 'mapping', {}),
            getattr(self, 'defaults', {}),
            getattr(self, 'validations', {}),
            getattr(self, 'declared_fields', {})
        )
        self._inherit(self, filter(None, (self.get_mapper(base) for base in self.model.__bases__)))  # recursive

        if not self.abstract:
            if not hasattr(self, 'db_table'):
                self.db_table = self._create_default_db_table(model)

            # fields and columns can be a descriptor for multilingual mapping.
            self.fields = collections.OrderedDict()
            self.columns = collections.OrderedDict()

            for name, field in self.create_fields(self._default_db().read_fields(self.db_table), self.declared_fields).items():
                self.add_field(name, field)

            self.pk = self._create_pk(self.db_table, self._default_db(), self.columns)
            self.sql_table = self._create_sql_table()

        self._prepare_model(model)
        self._setup_reverse_relations()

    def _create_default_name(self, model):
        return ".".join((model.__module__, model.__name__))

    def _create_default_db_table(self, model):
        return "_".join([
            re.sub(r"[^a-z0-9]", "", i.lower())
            for i in (model.__module__.split(".") + [model.__name__, ])
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

    def _create_pk(self, db_table, db, columns):
        pk = tuple(columns[i].name for i in db.read_pk(db_table))
        if pk:
            return pk[0] if len(pk) == 1 else pk
        return self.__class__.pk

    def _create_sql_table(self):
        return sql.Table(self)

    @property
    def base_query(self):
        """For relations."""
        return sql.Query(
            self.sql_table,
            result=self.result_factory(self, self._default_db())
        ).fields(
            self.get_sql_fields()
        )

    @property
    def query(self):
        """For selection."""
        return sql.Query(
            self.sql_table,
            result=self.result_factory(self, self._default_db())
        ).fields(
            self.get_sql_fields()
        )

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
        for related_mapper in self.mapper_registry.values():
            for key, rel in related_mapper.relations.items():
                try:
                    rel.setup_reverse_relation()
                except MapperNotRegistered:
                    pass

    def _inherit(self, successor, parents):
        for base in parents:  # recursive
            if not base.__dict__.get('polymorphic'):
                for name, field in base.declared_fields.items():
                    if name not in successor.declared_fields:
                        successor.declared_fields[name] = field

    @property
    def relations(self):  # bound_relations(), local_relations() ???
        result = {}
        for model in self.model.__mro__:
            for key, value in model.__dict__.items():
                if isinstance(value, interfaces.IRelationDescriptor):
                    result[key] = value.get_bound_relation(self.model)
        return result

    def load(self, data, db, from_db=True, reload=False):
        return Load(self, data, db, from_db, reload).compute()

    def unload(self, obj, fields=frozenset(), exclude=frozenset(), to_db=True):
        return Unload(self, obj, self._get_specified_fields(fields, exclude), to_db).compute()

    def make_identity_key(self, model, pk):
        return (model, to_tuple(pk))

    def get_changed(self, obj):
        if not self.original_data(obj):
            return set(self.fields)
        return frozenset(k for k, v in self.original_data(obj).items() if k in self.fields and self.fields[k].get_value(obj) != v)

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
        return smartsql.Insert(table=self.sql_table, mapping=data)

    def _update_query(self, obj):
        data = self.unload(obj, fields=self.get_changed(obj), to_db=True)
        data = {self.sql_table.get_field(k): v for k, v in data.items()}
        return smartsql.Update(table=self.sql_table, mapping=data, where=(self.sql_table.pk == self.get_pk(obj)))

    def _delete_query(self, obj):
        return smartsql.Delete(table=self.sql_table, where=(self.sql_table.pk == self.get_pk(obj)))

    def save(self, obj, db=None):
        """Sets defaults, validates and inserts into or updates database"""
        db = db or self._default_db()
        self.set_defaults(obj)
        self.validate(obj, fields=self.get_changed(obj))
        pre_save.send(sender=self.model, instance=obj, db=db)
        is_new = self.is_new(obj)
        result = self._insert(obj, db) if is_new else self._update(obj, db)
        # TODO: Set default values from DB?
        post_save.send(sender=self.model, instance=obj, created=is_new, db=db)
        self.original_data(obj, **self.unload(obj, to_db=False))
        self.is_new(obj, False)
        return result

    def _insert(self, obj, db):
        cursor = db.execute(self._insert_query(obj))
        if not all(to_tuple(self.get_pk(obj))):
            self.set_pk(obj, db.last_insert_id(cursor))
        self.used_db(obj, db)
        self.get_identity_map(db).add(self.make_identity_key(self.model, self.get_pk(obj)))

    def _update(self, obj, db):
        db.execute(self._update_query(obj))

    def delete(self, obj, db=None, visited=None):
        db = db or self._default_db()
        if visited is None:
            visited = set()
        if self in visited:
            return False
        visited.add(self)
        pre_delete.send(sender=self.model, instance=obj, db=db)
        for key, rel in self.relations.items():
            if isinstance(rel, OneToMany):
                for child in getattr(obj, key).iterator():
                    rel.on_delete(obj, child, rel, db, visited)
            elif isinstance(rel, OneToOne):
                try:
                    child = getattr(obj, key)
                except ObjectDoesNotExist:
                    pass
                else:
                    rel.on_delete(obj, child, rel, db, visited)

        db.execute(self._delete_query(obj))
        post_delete.send(sender=self.model, instance=obj, db=db)
        self.get_identity_map(db).remove(self.make_identity_key(self.model, self.get_pk(obj)))
        return True

    def get(self, _obj_pk=None, _db=None, **kwargs):
        if isinstance(_obj_pk, interfaces.IDatabase):
            _db, _obj_pk = _obj_pk, _db
        db = _db or self._default_db()
        if _obj_pk is not None:
            identity_map = self.get_identity_map(db)
            key = self.make_identity_key(self.model, _obj_pk)
            if identity_map.exists(key):
                return identity_map.get(key)
            try:
                obj = self.get(db, **{k: v for k, v in zip(to_tuple(self.pk), to_tuple(_obj_pk))})
            except ObjectDoesNotExist:
                identity_map.add(key)
                raise
            else:
                # obj added to identity_map by loader (self.load())
                return obj

        if kwargs:
            q = self.query.db(db)
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

    def get_identity_map(self, db):
        return db.identity_map

    def get_mapper(self, model_or_name, default=None):
        return self.mapper_registry.get(model_or_name, default)


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
        class_prepared.send(sender=self._model)

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

    def __init__(self, mapper, data, db, from_db, reload):
        """
        :type mapper: Mapper
        :type data: tuple
        :type db: ascetic.interfaces.IDatabase
        :type from_db: bool
        :type reload: bool
        """
        self._mapper = mapper
        self._data = data
        self._db = db
        self._from_db = from_db
        self._reload = reload

    def compute(self):
        if self._from_db:
            data_mapped = self._map_data_from_db(self._data)
        else:
            data_mapped = dict(self._data)
        key = self._mapper.make_identity_key(self._mapper.model, tuple(data_mapped[i] for i in to_tuple(self._mapper.pk)))
        try:
            obj = self._identity_map.get(key)
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
        self._mapper.used_db(obj, self._db)
        self._identity_map.add(key, obj)
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

    @property
    def _identity_map(self):
        return self._mapper.get_identity_map(self._db)


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
