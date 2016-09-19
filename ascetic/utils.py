from __future__ import absolute_import
import sys
import collections

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)


class UndefType(object):

    def __repr__(self):
        return "Undef"

    def __reduce__(self):
        return "Undef"

Undef = UndefType()


class cached_property(object):
    def __init__(self, func, name=None):
        self.func = func
        self.name = name or func.__name__

    def __get__(self, instance, type=None):
        if instance is None:
            return self
        res = instance.__dict__[self.name] = self.func(instance)
        return res


class classproperty(object):
    """Class property decorator"""
    def __init__(self, getter):
        self.getter = getter

    def __get__(self, instance, owner):
        return self.getter(owner)


def resolve(str_or_obj):
    """Returns object from string"""
    if not isinstance(str_or_obj, string_types):
        return str_or_obj
    if '.' not in str_or_obj:
        str_or_obj += '.'
    mod_name, obj_name = str_or_obj.rsplit('.', 1)
    __import__(mod_name)
    mod = sys.modules[mod_name]
    return getattr(mod, obj_name) if obj_name else mod


def to_tuple(val):
    return val if type(val) == tuple else (val,)


class SpecialAttrAccessor(object):
    # TODO: use WeakKeyDictionary?
    def __init__(self, key, default=None):
        self._key = self._prepare_key(key)
        self._default = default

    def _prepare_key(self, key):
        return '_{0}'.format(key)

    def set(self, obj, value):
        setattr(obj, self._key, value)

    def get(self, obj):
        try:
            return getattr(obj, self._key)
        except AttributeError:
            default = self._default
            if isinstance(default, collections.Callable):
                default = default()
            self.set(obj, default)
            return self.get(obj)

    def del_(self, obj):
        return delattr(obj, self._key)

    def __call__(self, obj, value=Undef):
        if value is Undef:
            return self.get(obj)
        else:
            self.set(obj, value)


class SpecialMappingAccessor(object):
    def __init__(self, attr_accessor):
        self.attr_accessor = attr_accessor

    def set(self, obj, data):
        self.attr_accessor.set(obj, data)

    def update(self, obj, *args, **kwargs):
        if args:
            data = args[0]
        else:
            data = kwargs
        self.get(obj).update(data)

    def get(self, obj):
        return self.attr_accessor.get(obj)

    def __call__(self, obj, *args, **kwargs):
        if args:
            data = args[0]
            self.set(obj, data)
        elif kwargs:
            data = kwargs
            self.update(obj, **data)
        else:
            return self.get(obj)
