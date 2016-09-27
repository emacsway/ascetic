from ascetic.utils import Undef


class IRelation(object):

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
    def rel(self):
        raise NotImplementedError

    @property
    def rel_name(self):
        raise NotImplementedError

    @property
    def rel_model(self):
        raise NotImplementedError

    @property
    def rel_field(self):
        raise NotImplementedError

    @property
    def rel_query(self):
        raise NotImplementedError

    def get_where(self, rel_obj):
        raise NotImplementedError

    def get_rel_where(self, obj):
        raise NotImplementedError

    def get_join_where(self):
        raise NotImplementedError

    def get_value(self, obj):
        raise NotImplementedError

    def get_rel_value(self, rel_obj):
        raise NotImplementedError

    def set_value(self, obj, value):
        raise NotImplementedError

    def set_rel_value(self, rel_obj, value):
        raise NotImplementedError

    def setup_related(self):
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
