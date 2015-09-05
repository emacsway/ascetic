from __future__ import absolute_import
import re

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)


class ValidationError(Exception):
    pass


class Validator(object):
    pass


class Required(Validator):
    empty_values = (None, '')
    msg = False

    def __init__(self, msg):
        self.msg = msg

    def __call__(self, value):
        return value not in self.empty_values or self.msg


class Regex(Validator):
    def __call__(self, value):
        return bool(self.regex.match(value))


class Email(Regex):
    regex = re.compile(r'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.(?:[A-Z]{2}|com|org|net|gov|mil|biz|info|mobi|name|aero|jobs|museum)$', re.I)


class Length(Validator):
    def __init__(self, min_length=1, max_length=None):
        if max_length is not None:
            assert max_length >= min_length, "max_length must be greater than or equal to min_length"
        self.min_length = min_length
        self.max_length = max_length

    def __call__(self, string):
        l = len(str(string))
        return (l >= self.min_length) and \
               (self.max_length is None or l <= self.max_length)


class Number(Validator):
    def __init__(self, minimum=None, maximum=None):
        if None not in (minimum, maximum):
            assert maximum >= minimum, "maximum must be greater than or equal to minimum"
        self.minimum = minimum
        self.maximum = maximum

    def __call__(self, number):
        return isinstance(number, integer_types + (float, complex,)) and \
            (self.minimum is None or number >= self.minimum) and \
            (self.maximum is None or number <= self.maximum)


class ValidatorChain(object):
    def __init__(self, *validators):
        self.validators = validators

    def __call__(self, *a, **kw):
        for validator in self.validators:
            test = validator(*a, **kw)
            if test is not True:
                return test
        return True
