import weakref
from ascetic import interfaces
from ascetic.databases import databases
from ascetic.exceptions import ObjectDoesNotExist


class NonexistentObject(object):
    pass


class CacheLru(object):

    def __init__(self, size=1000):
        self._order = []
        self._size = size

    def add(self, value):
        self._order.append(value)
        if len(self._order) > self._size:
            self._order.pop(0)

    def touch(self, value):
        obj = value
        try:
            obj = self._order.pop(self._order.index(obj))
        except (ValueError, IndexError):
            pass
        self._order.append(obj)

    def remove(self, value):
        try:
            self._order.remove(value)
        except IndexError:
            pass

    def clear(self):
        del self._order[:]

    def set_size(self, size):
        self._size = size


class IStrategy(object):

    def add(self, key, value=None):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def exists(self, key):
        raise NotImplementedError


class BaseStrategy(IStrategy):
    def __init__(self, identity_map):
        """
        :type identity_map: IdentityMap
        """
        self._identity_map = weakref.ref(identity_map)


class ReadUncommittedStrategy(BaseStrategy):

    def add(self, key, value=None):
        pass

    def get(self, key):
        raise KeyError

    def exists(self, key):
        return False


class ReadCommittedStrategy(BaseStrategy):

    def add(self, key, value=None):
        pass

    def get(self, key):
        raise KeyError

    def exists(self, key):
        return False


class RepeatableReadsStrategy(BaseStrategy):

    def add(self, key, value=None):
        if value is not None:
            self._identity_map().do_add(key, value)

    def get(self, key):
        obj = self._identity_map().do_get(key)
        if isinstance(obj, NonexistentObject):
            raise KeyError
        return obj

    def exists(self, key):
        try:
            obj = self._identity_map().do_get(key)
        except KeyError:
            return False
        else:
            return not isinstance(obj, NonexistentObject)


class SerializableStrategy(BaseStrategy):

    def add(self, key, value=None):
        if value is None:
            value = NonexistentObject()
        self._identity_map().do_add(key, value)

    def get(self, key):
        obj = self._identity_map().do_get(key)
        if isinstance(obj, NonexistentObject):
            raise ObjectDoesNotExist()
        return obj

    def exists(self, key):
        try:
            self._identity_map().do_get(key)
        except KeyError:
            return False
        else:
            return True


class IdentityMap(interfaces.IIdentityMap):

    READ_UNCOMMITTED = 0  # IdentityMap is disabled
    READ_COMMITTED = 1  # IdentityMap is disabled
    REPEATABLE_READS = 2  # Prevent repeated DB-query only for existent objects
    SERIALIZABLE = 3  # Prevent repeated DB-query for both, existent and nonexistent objects

    STRATEGY_MAP = {
        READ_UNCOMMITTED: ReadUncommittedStrategy,
        READ_COMMITTED: ReadCommittedStrategy,
        REPEATABLE_READS: RepeatableReadsStrategy,
        SERIALIZABLE: SerializableStrategy,
    }
    _default_isolation_level = SERIALIZABLE

    """
    def __new__(cls, db_accessor, *args, **kwargs):
        if not hasattr(databases[alias], 'identity_map'):
            self = databases[alias].identity_map = object.__new__(cls)
            self.db = db_accessor
            self.cache = CacheLru()
            self.alive = weakref.WeakValueDictionary()
            self.set_isolation_level(self._default_isolation_level)
        return databases[alias].identity_map
    """

    def __init__(self, db_accessor, *args, **kwargs):
        self.db = db_accessor
        self.cache = CacheLru()
        self.alive = weakref.WeakValueDictionary()
        self.set_isolation_level(self._default_isolation_level)
        self._disposable = self._subscribe(self.db())

    def add(self, key, value=None):
        return self._strategy.add(key, value)

    def get(self, key):
        return self._strategy.get(key)

    def exists(self, key):
        return self._strategy.exists(key)

    def do_add(self, key, value=None):
        self.cache.add(value)
        self.alive[key] = value

    def do_get(self, key):
        value = self.alive[key]
        self.cache.touch(value)
        return value

    def remove(self, key):
        try:
            value = self.alive[key]
            self.cache.remove(value)
            del self.alive[key]
        except KeyError:
            pass

    def clear(self):
        self.cache.clear()
        self.alive.clear()

    def sync(self):
        Sync(self).compute()

    def set_isolation_level(self, level):
        self._isolation_level = level
        self._strategy = self.STRATEGY_MAP[level](self)

    def enable(self):
        if hasattr(self, '_last_isolation_level'):
            self.set_isolation_level(self._last_isolation_level)
            del self._last_isolation_level

    def disable(self):
        if not hasattr(self, '_last_isolation_level'):
            self._last_isolation_level = self._isolation_level
            self.set_isolation_level(self.READ_UNCOMMITTED)

    def _subscribe(self, subject):
        return (
            subject.observed().attach('commit', self._on_commit) +
            subject.observed().attach('rollback', self._on_rollback)
        )

    def _on_commit(self, subject, aspect):
        self.sync()

    def _on_rollback(self, subject, aspect):
        self.sync()


class Sync(object):
    def __init__(self, identity_map):
        """
        :type identity_map: ascetic.interfaces.IIdentityMap
        """
        self._identity_map = identity_map

    def _sync(self):
        for model, model_object_map in self._get_typed_objects():
            mapper = self._get_mapper(model)
            pks = list(model_object_map.values())
            for obj in self._make_query(mapper, pks):
                assert mapper.get_pk(obj) in model_object_map
                assert not mapper.get_changed(obj)

    def _get_typed_objects(self):
        typed_objects = {}
        for obj in self._identity_map.alive.values():
            model = obj.__class__
            if model not in typed_objects:
                typed_objects[model] = {}
            mapper = self._get_mapper(model)
            typed_objects[model][mapper.get_pk(obj)] = obj
        return typed_objects

    def _make_query(self, mapper, pks):
        db = self._identity_map.db()
        query = mapper.query.db(db).where(mapper.sql_table.pk.in_(pks))
        query = query.map(lambda result, row, state: result.mapper.load(row, db, from_db=True, reload=True))
        return query

    def _get_mapper(self, model_or_name):
        from ascetic.mappers import mapper_registry
        return mapper_registry[model_or_name]

    def compute(self):
        self._sync()
