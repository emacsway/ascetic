from functools import wraps
from uuid import uuid4
from ascetic import interfaces, utils
from ascetic.databases import databases


class BaseTransaction(interfaces.ITransaction):
    def __init__(self, db_accessor):
        self._db = db_accessor

    def parent(self):
        return None

    def can_reconnect(self):
        return False

    def set_autocommit(self, autocommit):
        raise Exception("You cannot set autocommit during a managed transaction!")

    @utils.cached_property
    def _identity_map(self):
        return self._db().transaction.identity_map


class Transaction(BaseTransaction):

    def begin(self):
        self._db().execute("BEGIN")

    def commit(self):
        self._db().commit()
        self._reset_identity_map()

    def rollback(self):
        self._db().rollback()
        self._reset_identity_map()

    def _reset_identity_map(self):
        self._identity_map.clear()


class SavePoint(BaseTransaction):
    def __init__(self, using, parent, name=None):
        BaseTransaction.__init__(self, using)
        self._parent = parent
        self._name = name or 's' + uuid4().hex

    def parent(self):
        return self._parent

    def begin(self):
        self._db().begin_savepoint(self._name)

    def commit(self):
        self._db().commit_savepoint(self._name)

    def rollback(self):
        self._db().rollback_savepoint(self._name)


class NoneTransaction(BaseTransaction):
    def begin(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def can_reconnect(self):
        return True

    def set_autocommit(self, autocommit):
        self._db().set_autocommit(autocommit)


class TransactionManager(interfaces.ITransactionManager):
    """
    :type identity_map: ascetic.interfaces.IIdentityMap
    """
    def __init__(self, db_accessor, autocommit, identity_map):
        """
        :type db_accessor:
        :type autocommit:
        :type identity_map: ascetic.interfaces.IIdentityMap
        """
        self._db = db_accessor
        self._current = None
        self._autocommit = autocommit
        self.identity_map = identity_map

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
            return self._current or NoneTransaction(self._db)
        self._current = node

    def begin(self):
        if self._current is None:
            self.current().set_autocommit(False)
            self.current(Transaction(self._db))
        else:
            self.current(SavePoint(self._db, self.current()))
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
