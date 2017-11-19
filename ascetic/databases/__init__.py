import os
from threading import local
from sqlbuilder import smartsql
from ascetic import settings
from ascetic.databases.base import Database

# Register backends
__import__('ascetic.databases.mysql')
__import__('ascetic.databases.sqlite')
__import__('ascetic.databases.postgresql')

try:
    import _thread
except ImportError:
    import thread as _thread  # Python < 3.*


class Databases(object):

    def __init__(self, conf):
        self._settings = conf
        self._databases = local()

    def create_database(self, alias):
        return Database.factory(alias=alias, **self._settings[alias])

    @staticmethod
    def get_thread_id():
        """Returs id for current thread."""
        return (os.getpid(), _thread.get_ident())

    def close(self):
        for alias in self:
            del self[alias]

    def __getitem__(self, alias):
        try:
            # Prevent situation like this:
            # http://stackoverflow.com/a/7285933
            # http://stackoverflow.com/questions/7285541/pythons-multiprocessing-does-not-play-nicely-with-threading-local
            # A fork() completely duplicates the process object, along with its
            # memory, loaded code, open file descriptors and threads.
            # Moreover, the new process usually shares the very same process
            # object within the kernel until the first memory write operation.
            # This basically means that the local data structures are also being
            # copied into the new process, along with the thread local variables.
            # return getattr(self._databases, alias)
            db = getattr(self._databases, alias)
            if db._thread_id != _thread.get_ident():
                raise ValueError
            return db
        except (AttributeError, ValueError):
            db = self.create_database(alias)
            db._thread_id = _thread.get_ident()
            setattr(self._databases, alias, db)
            return db

    def __delitem__(self, alias):
        if hasattr(self._databases, alias):
            getattr(self._databases, alias).close()
            delattr(self._databases, alias)

    def __iter__(self):
        return iter(self._settings)


databases = Databases(settings.DATABASES)
