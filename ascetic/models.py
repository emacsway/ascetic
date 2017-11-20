from ascetic.mappers import Mapper, thread_safe
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
        new_cls._mapper = mapper_factory(new_cls)
        for k in to_tuple(new_cls._mapper.pk):
            setattr(new_cls, k, None)

        return new_cls


class Model(ModelBase("NewBase", (object, ), {})):

    _new_record = True
    _s = None

    def __init__(self, *args, **kwargs):
        pre_init.send(sender=self.__class__, instance=self, args=args, kwargs=kwargs)
        if args:
            self.__dict__.update(zip(self._mapper.fields.keys(), args))
        if kwargs:
            self.__dict__.update(kwargs)
        post_init.send(sender=self.__class__, instance=self)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._get_pk() == other._get_pk()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if not all(to_tuple(self._get_pk())):
            raise TypeError("Model instances without primary key value are unhashable")
        return hash(self._get_pk())

    def __dir__(self):
        return dir(super(Model, self)) + list(self._mapper.fields)

    def _get_pk(self):
        return self._mapper.get_pk(self)

    def _set_pk(self, value):
        self._mapper.set_pk(self, value)

    pk = property(_get_pk, _set_pk)

    def validate(self, fields=frozenset(), exclude=frozenset()):
        return self._mapper.validate(self, fields=fields, exclude=exclude)

    def save(self, *args, **kwargs):
        return self._mapper.save(self, *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self._mapper.delete(self, *args, **kwargs)

    @classproperty
    def s(cls):
        # TODO: Use Model class descriptor without __set__().
        return cls._mapper.sql_table

    @classproperty
    def q(cls):
        return cls._mapper.query

    @classmethod
    def get(cls, *args, **kwargs):
        return cls._mapper.get(*args, **kwargs)

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
