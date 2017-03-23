from __future__ import absolute_import
import re
import collections
from ascetic.exceptions import ValidationError

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)


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


class ChainValidator(object):
    def __init__(self, *validators):
        self.validators = validators

    def __call__(self, value):
        errors = []
        for validator in self.validators:
            assert isinstance(validator, collections.Callable), 'The validator must be callable'
            try:
                valid_or_msg = validator(value)
            except ValidationError as e:
                errors.append(e.args[0])
            else:
                if valid_or_msg is False:
                    errors.append('Improper value "{0!r}"'.format(value))
                if isinstance(valid_or_msg, string_types):
                    # Don't need message code. To rewrite message simple wrap (or extend) validator.
                    errors.append(valid_or_msg)
        if errors:
            raise ValidationError(errors)
        return True


class MappingValidator(object):
    def __init__(self, *args, **kwargs):
        self.validators = kwargs or args[0]

    def __call__(self, items):
        errors = {}
        for name, validator in self.validators.items():
            try:
                validator(items.get(name))
            except ValidationError as e:
                errors[name] = e.args[0]
        if errors:
            raise ValidationError(errors)


class CompositeMappingValidator(object):
    def __init__(self, *validators):
        self.validators = validators

    def __call__(self, items):
        errors = {}
        for validator in self.validators:
            try:
                validator(items)
            except ValidationError as e:
                for k, v in e.args[0].items():
                    errors.setdefault(k, []).extend(v)
        if errors:
            raise ValidationError(errors)
