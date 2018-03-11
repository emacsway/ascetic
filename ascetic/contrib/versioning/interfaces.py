class IRegistry(object):
    def register(self, model, fields, object_accessor):
        """
        :type model: object
        :type fields: list[str]
        :type object_accessor: IObjectAccessor
        """
        raise NotImplementedError

    def unregister(self, model):
        """
        :type model: object
        """
        raise NotImplementedError

    def get_fields(self, model):
        """
        :type model: object
        :rtype: list[str]
        """
        raise NotImplementedError

    def get_object_accessor(self, model):
        """
        :type model: object
        :rtype: IObjectAccessor
        """
        raise NotImplementedError


class IChangesetRepository(object):
    def next(self, subset_of_stamp=None):
        raise NotImplementedError


class IRevisionRepository(object):
    def commit(self, obj, stamp):
        raise NotImplementedError

    def versions(self, obj):
        raise NotImplementedError

    def version(self, obj, stamp=None):
        raise NotImplementedError

    def object_version(self, obj, stamp=None):
        raise NotImplementedError


class IComparator(object):
    def create_delta(self, prev_obj, next_obj):
        """
        :type prev_obj: object
        :type next_obj: object
        :rtype: collections.Mapping
        """
        raise NotImplementedError

    def apply_delta(self, obj, delta):
        """
        :type obj: object
        :type delta: collections.Mapping
        """
        raise NotImplementedError

    def is_equal(self, prev_obj, next_obj):
        """
        :type prev_obj: object
        :type next_obj: object
        :rtype: bool
        """
        raise NotImplementedError

    def display_diff(self, prev_obj, next_obj):
        """
        Returns a HTML representation of the diff.

        :type prev_obj: object
        :type next_obj: object
        :rtype: str
        """
        raise NotImplementedError


class ISerializer(object):
    """
    :type VERSION: int
    """
    VERSION = None

    def is_acceptable(self, dump):
        """
        :type dump: string
        :rtype: bool
        """
        raise NotImplementedError

    def dumps(self, payload):
        """
        :type payload: collections.Mapping
        :rtype: string
        """
        raise NotImplementedError

    def loads(self, dump):
        """
        :type dump: string
        :rtype: collections.Mapping
        """
        raise NotImplementedError


class ICodecAdapter(object):

    def decode(self, encoded):
        """
        :type encoded: str
        :rtype: object
        """
        raise NotImplementedError

    def encode(self, decoded):
        """
        :type decoded: object
        :rtype: str
        """
        raise NotImplementedError


class IObjectAccessor(object):
    def get_pk(self, obj):
        """
        :type obj: object
        :rtype: str
        """
        raise NotImplementedError

    def get_content_type(self, model=None):
        """
        :type model: object
        :rtype: str
        """
        raise NotImplementedError

    def get_value(self, obj, field):
        """
        :type obj: object
        :type field: str
        :rtype: object
        """
        raise NotImplementedError

    def set_value(self, obj, field, value):
        """
        :type obj: object
        :type field: str
        :type value: object
        """
        raise NotImplementedError


class ITransaction(object):
    def add_object(self, obj):
        raise NotImplementedError

    def begin(self):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def rollback(self):
        raise NotImplementedError

    def is_null(self):
        """
        :rtype: bool
        """
        raise NotImplementedError


class ITransactionManager(object):

    def __call__(self, func=None):
        raise NotImplementedError

    def __enter__(self):
        raise NotImplementedError

    def __exit__(self, *args):
        raise NotImplementedError

    def add_object(self, obj):
        raise NotImplementedError

    def begin(self):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def rollback(self):
        raise NotImplementedError
