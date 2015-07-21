from __future__ import absolute_import
import sys

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)


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
