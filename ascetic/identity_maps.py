import weakref

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

    def _do_add(self, key, value=None):
        self._identity_map().cache.add(value)
        self._identity_map().alive[key] = value

    def _do_get(self, key):
        value = self._identity_map().alive[key]
        self._identity_map().cache.touch(value)
        return value


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
            self._do_add(key, value)

    def get(self, key):
        value = self._do_get(key)
        if value.__class__ is NonexistentObject:
            raise KeyError
        return value

    def exists(self, key):
        return self._identity_map().alive.get(key).__class__ not in (NonexistentObject, type(None))


class SerializableStrategy(BaseStrategy):

    def add(self, key, value=None):
        if value is None:
            value = NonexistentObject()
        self._do_add(key, value)

    def get(self, key):
        value = self._do_get(key)
        if value.__class__ == NonexistentObject:
            raise ObjectDoesNotExist()
        return value

    def exists(self, key):
        return key in self._identity_map().alive


class IdentityMap(object):

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

    def __new__(cls, alias='default', *args, **kwargs):
        if not hasattr(databases[alias], 'identity_map'):
            self = databases[alias].identity_map = object.__new__(cls)
            self.cache = CacheLru()
            self.alive = weakref.WeakValueDictionary()
            self.set_isolation_level(self._default_isolation_level)
        return databases[alias].identity_map

    def add(self, key, value=None):
        return self._strategy.add(key, value)

    def get(self, key):
        return self._strategy.get(key)

    def exists(self, key):
        return self._strategy.exists(key)

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
