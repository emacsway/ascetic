from functools import wraps
from uuid import uuid4
from ascetic import interfaces, utils
from ascetic.databases import databases


class BaseTransaction(interfaces.ITransaction):
    def __init__(self, using):
        self._using = using

    def parent(self):
        return None

    def can_reconnect(self):
        return False

    def set_autocommit(self, autocommit):
        raise Exception("You cannot set autocommit during a managed transaction!")

    @utils.cached_property
    def _db(self):
        return databases[self._using]


class Transaction(BaseTransaction):

    def begin(self):
        self._db.execute("BEGIN")

    def commit(self):
        self._db.commit()
        self._clear_identity_map()

    def rollback(self):
        self._db.rollback()
        self._clear_identity_map()

    def _clear_identity_map(self):
        from ascetic.identity_maps import IdentityMap
        IdentityMap(self._using).clear()


class SavePoint(BaseTransaction):
    def __init__(self, using, parent, name=None):
        BaseTransaction.__init__(self, using)
        self._parent = parent
        self._name = name or 's' + uuid4().hex

    def parent(self):
        return self._parent

    def begin(self, name=None):
        self._db.begin_savepoint(self._name)

    def commit(self):
        self._db.commit_savepoint(self._name)

    def rollback(self):
        self.rollback_savepoint(self._name)


class NoneTransaction(BaseTransaction):
    def begin(self, name=None):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def can_reconnect(self):
        return True

    def set_autocommit(self, autocommit):
        self._db.set_autocommit(autocommit)


class TransactionManager(interfaces.ITransactionManager):
    def __init__(self, using, autocommit):
        self._using = using
        self._current = None
        self._autocommit = autocommit

    def __call__(self, func=None):
        if func is None:
            return self

        @wraps(func)
        def _decorated(*a, **kw):
            with self:
                rv = func(*a, **kw)
            return rv

        return _decorated

    def __enter__(self):
        self.begin()

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                self.rollback()
            else:
                try:
                    self.commit()
                except:
                    self.rollback()
                    raise
        finally:
            pass

    def current(self, node=utils.Undef):
        if node is utils.Undef:
            return self._current or NoneTransaction(self._using)
        self._current = node

    def begin(self):
        if self._current is None:
            self.current().set_autocommit(False)
            self.current(Transaction(self._using))
        else:
            self.current(SavePoint(self._using, self.current()))
        self.current().begin()
        return

    def commit(self):
        self.current().commit()
        self.current(self.current().parent())

    def rollback(self):
        self.current().rollback()
        self.current(self.current().parent())

    def can_reconnect(self):
        return self.current().can_reconnect()

    def on_connect(self):
        self._current = None
        self.current().set_autocommit(self._autocommit)

    def autocommit(self, autocommit=None):
        if autocommit is None:
            return self._autocommit and not self._current
        self._autocommit = autocommit
        self.current().set_autocommit(autocommit)
