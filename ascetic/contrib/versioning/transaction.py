from ascetic.contrib.versioning.interfaces import ITransaction, ITransactionManager


class DummyTransaction(ITransaction):
    def __init__(self, repository, version_stamp_sequence):
        """
        :type repository: ascetic.contrib.versioning.interfaces.IRepository
        :type version_stamp_sequence: ascetic.contrib.versioning.interfaces.IVersionStampSequence
        """
        self._repository = repository
        self._version_stamp_sequence = version_stamp_sequence

    def add_object(self, obj):
        pass

    def begin(self):
        return Transaction(self._repository, self._version_stamp_sequence)

    def commit(self):
        return self

    def rollback(self):
        return self

    def is_null(self):
        return True


class Transaction(ITransaction):
    def __init__(self, repository, version_stamp_sequence):
        """
        :type repository: ascetic.contrib.versioning.interfaces.IRepository
        :type version_stamp_sequence: ascetic.contrib.versioning.interfaces.IVersionStampSequence
        """
        self._repository = repository
        self._version_stamp_sequence = version_stamp_sequence
        self._stamp = next(self._version_stamp_sequence)
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
                self._repository.commit(obj, self._stamp)
                self._committed.add(obj_id)

    def rollback(self):
        return DummyTransaction(self._repository, self._version_stamp_sequence)

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
