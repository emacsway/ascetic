from ascetic.utils import Undef


class ITransaction(object):

    def parent(self):
        raise NotImplementedError

    def begin(self):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def rollback(self):
        raise NotImplementedError

    def can_reconnect(self):
        raise NotImplementedError

    def set_autocommit(self, autocommit):
        raise NotImplementedError


class ITransactionManager(object):

    def __call__(self, func=None):
        raise NotImplementedError

    def __enter__(self):
        raise NotImplementedError

    def __exit__(self, *args):
        raise NotImplementedError

    def current(self, node=Undef):
        raise NotImplementedError

    def begin(self):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def rollback(self):
        raise NotImplementedError

    def can_reconnect(self):
        raise NotImplementedError

    def on_connect(self):
        raise NotImplementedError

    def autocommit(self, autocommit=None):
        raise NotImplementedError
