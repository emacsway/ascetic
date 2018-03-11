from ascetic.contrib.versioning.interfaces import ITransaction, ITransactionManager


class DummyTransaction(ITransaction):
    def __init__(self, changeset_repository, revision_repository):
        """
        :type changeset_repository: ascetic.contrib.versioning.interfaces.IChangesetRepository
        :type revision_repository: ascetic.contrib.versioning.interfaces.IRevisionRepository
        """
        self._revision_repository = revision_repository
        self._changeset_repository = changeset_repository

    def add_object(self, obj):
        pass

    def begin(self):
        return Transaction(self._changeset_repository, self._revision_repository)

    def commit(self):
        return self

    def rollback(self):
        return self

    def is_null(self):
        return True


class Transaction(ITransaction):
    def __init__(self, changeset_repository, revision_repository):
        """
        :type changeset_repository: ascetic.contrib.versioning.interfaces.IChangesetRepository
        :type revision_repository: ascetic.contrib.versioning.interfaces.IRevisionRepository
        """
        self._revision_repository = revision_repository
        self._changeset_repository = changeset_repository
        self._stamp = self._changeset_repository.next()
        self._objects = list()
        self._committed = set()

    def add_object(self, obj):
        self._objects.append(obj)

    def begin(self):
        return SavePoint(self)

    def commit(self):
        for obj in self._objects:
            obj_id = self._get_object_id(obj)
            if obj_id not in self._committed:
                self._revision_repository.commit(obj, self._stamp)
                self._committed.add(obj_id)

    def rollback(self):
        return DummyTransaction(self._changeset_repository, self._revision_repository)

    def is_null(self):
        return False

    @staticmethod
    def _get_object_id(obj):
        try:
            return 0, hash(obj)
        except TypeError:
            return 1, id(obj)


class SavePoint(ITransaction):
    def __init__(self, parent):
        """
        :type parent: ascetic.contrib.versioning.interfaces.ITransaction
        """
        self._parent = parent
        self._objects = list()

    def add_object(self, obj):
        self._objects.append(obj)

    def begin(self):
        return SavePoint(self)

    def commit(self):
        for obj in self._objects:
            self._parent.add_object(obj)
        return self._parent

    def rollback(self):
        return self._parent

    def is_null(self):
        return False
