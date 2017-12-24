from ascetic.utils import Undef


class IObservable(object):
    def attach(self, aspects, observer):
        """
        :type aspects: collections.Hashable or list[collections.Hashable]
        :type observer: callable
        :rtype: ascetic.interfaces.IDisposable
        """
        raise NotImplementedError

    def detach(self, aspects, observer):
        """
        :type aspects: collections.Hashable or list[collections.Hashable]
        :type observer: callable
        """
        raise NotImplementedError

    def notify(self, aspect, *args, **kwargs):
        """
        :type aspect: collections.Hashable
        """
        raise NotImplementedError

    def is_null(self):
        """
        :rtype: bool
        """
        raise NotImplementedError


class IDisposable(object):
    def dispose(self):
        raise NotImplementedError

    def __add__(self, other):
        raise NotImplementedError


class IDatabase(object):
    """
    :type transaction: ascetic.interfaces.ITransaction
    :type identity_map: ascetic.interfaces.IIdentityMap
    :type identity_map: () -> ascetic.interfaces.IObservable
    """
    transaction = None
    identity_map = None
    observed = None

    def connection_factory(self, **kwargs):
        raise NotImplementedError

    def _ensure_connected(self):
        """
        :rtype: ascetic.interfaces.IDatabase
        """
        raise NotImplementedError

    def execute(self, sql, params=()):
        """
        :type sql: str
        :type params: collections.abc.Iterable
        :rtype: sqlite3.Cursor
        """
        raise NotImplementedError

    def cursor(self):
        """
        :rtype: sqlite3.Cursor
        """
        raise NotImplementedError

    def last_insert_id(self, cursor):
        raise NotImplementedError

    def begin(self):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def rollback(self):
        raise NotImplementedError

    def begin_savepoint(self, name):
        """
        :type name: str
        """
        raise NotImplementedError

    def commit_savepoint(self, name):
        """
        :type name: str
        """
        raise NotImplementedError

    def rollback_savepoint(self, name):
        """
        :type name: str
        """
        raise NotImplementedError

    def set_autocommit(self, autocommit):
        """
        :type autocommit: bool
        """
        raise NotImplementedError

    def read_pk(self, db_table):
        """
        :type db_table: str
        :rtype: tuple
        """
        raise NotImplementedError

    def read_fields(self, db_table):
        """
        :type db_table: str
        :rtype: dict
        """
        raise NotImplementedError

    def describe_table(self, db_table):
        """
        :type db_table: str
        :rtype: dict
        """
        raise NotImplementedError

    def qn(self, name):
        """
        :type name: str
        :rtype: str
        """

    def close(self):
        raise NotImplementedError

    @classmethod
    def register(cls, engine):
        """
        :type engine: str
        :rtype: collections.abc.Callable
        """
        raise NotImplementedError

    @classmethod
    def factory(cls, **kwargs):
        """
        :rtype: ascetic.interfaces.IDatabase
        """
        raise NotImplementedError


class IBaseRelation(object):

    # @property
    # def field(self):
    #     raise NotImplementedError

    def setup_reverse_relation(self):
        """
        :return bool: True if operation is successful else False
        """
        raise NotImplementedError

    def bind(self, owner):
        """Using Prototype pattern.
        :param owner: type
        :return: IRelation
        """
        raise NotImplementedError

    def get(self, instance):
        raise NotImplementedError

    def set(self, instance, value):
        raise NotImplementedError

    def delete(self, instance):
        raise NotImplementedError


class IRelation(IBaseRelation):

    @property
    def name(self):
        raise NotImplementedError

    @property
    def model(self):
        raise NotImplementedError

    @property
    def field(self):
        raise NotImplementedError

    @property
    def query(self):
        raise NotImplementedError

    @property
    def related_relation(self):
        raise NotImplementedError

    @property
    def related_name(self):
        raise NotImplementedError

    @property
    def related_model(self):
        raise NotImplementedError

    @property
    def related_field(self):
        raise NotImplementedError

    @property
    def related_query(self):
        raise NotImplementedError

    def get_where(self, related_obj):
        raise NotImplementedError

    def get_related_where(self, obj):
        raise NotImplementedError

    def get_join_where(self, prefix=None, related_prefix=None):
        """
        :type prefix: ascetic.query.Table
        :type related_prefix: ascetic.query.Table
        :rtype: sqlbuilder.smartsql.expressions.Operable
        """
        raise NotImplementedError

    def get_value(self, obj):
        raise NotImplementedError

    def get_related_value(self, related_obj):
        raise NotImplementedError

    def set_value(self, obj, value):
        raise NotImplementedError

    def set_related_value(self, related_obj, value):
        raise NotImplementedError


class IRelationDescriptor(object):
    def get_bound_relation(self, owner):
        """
        :type owner: type
        :rtype: IRelation
        """
        raise NotImplementedError

    def __get__(self, instance, owner):
        raise NotImplementedError

    def __set__(self, instance, value):
        raise NotImplementedError

    def __delete__(self, instance):
        raise NotImplementedError


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

    def autocommit(self, autocommit=None):
        raise NotImplementedError


class IIdentityMap(object):

    def add(self, key, value=None):
        """
        :type key: collections.Hashable
        :type value: object or None
        :rtype: object or None
        """
        raise NotImplementedError

    def get(self, key):
        """
        :type key: collections.Hashable
        :rtype: object or None
        """
        raise NotImplementedError

    def exists(self, key):
        """
        :type key: collections.Hashable
        :rtype: bool
        """
        raise NotImplementedError

    def do_add(self, key, value=None):
        """
        :type key: collections.Hashable
        :type value: object or None
        """
        raise NotImplementedError

    def do_get(self, key):
        """
        :type key: collections.Hashable
        :rtype: object or None
        """
        raise NotImplementedError

    def remove(self, key):
        """
        :type key: collections.Hashable
        """
        raise NotImplementedError

    def clear(self):
        raise NotImplementedError

    def sync(self):
        raise NotImplementedError

    def set_isolation_level(self, level):
        raise NotImplementedError

    def enable(self):
        raise NotImplementedError

    def disable(self):
        raise NotImplementedError
