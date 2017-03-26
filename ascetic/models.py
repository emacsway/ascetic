from sqlbuilder import smartsql

from ascetic.mappers import mapper_registry, Mapper, thread_safe
from ascetic.signals import pre_init, post_init
from ascetic.utils import classproperty, to_tuple


class ModelBase(type):
    """Metaclass for Model"""
    mapper_class = Mapper

    @thread_safe
    def __new__(mcs, name, bases, attrs):

        new_cls = type.__new__(mcs, name, bases, attrs)

        if name in ('Model', 'NewBase', ):
            return new_cls

        mapper_class = getattr(new_cls, 'Mapper', None) or getattr(new_cls, 'Meta', None)
        bases = []
        if mapper_class is not None:
            bases.append(mapper_class)
        if not isinstance(mapper_class, new_cls.mapper_class):
            bases.append(new_cls.mapper_class)

        mapper_factory = type("{}Mapper".format(new_cls.__name__), tuple(bases), {})
        mapper_factory(new_cls)
        for k in to_tuple(mapper_registry[new_cls].pk):
            setattr(new_cls, k, None)

        return new_cls


class Model(ModelBase("NewBase", (object, ), {})):

    _new_record = True
    _s = None

    def __init__(self, *args, **kwargs):
        mapper = mapper_registry[self.__class__]
        pre_init.send(sender=self.__class__, instance=self, args=args, kwargs=kwargs, using=mapper.using())
        if args:
            self.__dict__.update(zip(mapper.fields.keys(), args))
        if kwargs:
            self.__dict__.update(kwargs)
        post_init.send(sender=self.__class__, instance=self, using=mapper.using())

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._get_pk() == other._get_pk()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if not all(to_tuple(self._get_pk())):
            raise TypeError("Model instances without primary key value are unhashable")
        return hash(self._get_pk())

    def __dir__(self):
        return dir(super(Model, self)) + list(mapper_registry[self.__class__].fields)

    def _get_pk(self):
        return mapper_registry[self.__class__].get_pk(self)

    def _set_pk(self, value):
        mapper_registry[self.__class__].set_pk(self, value)

    pk = property(_get_pk, _set_pk)

    def validate(self, fields=frozenset(), exclude=frozenset()):
        return mapper_registry[self.__class__].validate(self, fields=fields, exclude=exclude)

    def save(self, using=None):
        return mapper_registry[self.__class__].using(using).save(self)

    def delete(self, using=None, visited=None):
        return mapper_registry[self.__class__].using(using).delete(self, visited)

    @classproperty
    def _mapper(cls):
        return mapper_registry[cls]

    @classproperty
    def s(cls):
        # TODO: Use Model class descriptor without __set__().
        return mapper_registry[cls].sql_table

    @classproperty
    def q(cls):
        return mapper_registry[cls].query

    @classproperty
    def qs(cls):
        smartsql.warn('Model.qs', 'Model.q')
        return mapper_registry[cls].query

    @classmethod
    def get(cls, _obj_pk=None, **kwargs):
        return mapper_registry[cls].get(_obj_pk, **kwargs)

    def __repr__(self):
        return "<{0}.{1}: {2}>".format(type(self).__module__, type(self).__name__, self.pk)


class CompositeModel(object):
    """Composite model.

    Exaple of usage:
    >>> rows = CompositeModel(Model1, Model2).q...filter(...)
    >>> type(rows[0]):
        CompositeModel
    >>> list(rows[0])
        [<Model1: 1>, <Model2: 2>]
    """
    def __init__(self, *models):
        self.models = models

    # TODO: build me.
