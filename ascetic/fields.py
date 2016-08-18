import collections

from ascetic.utils import Undef
from ascetic.validators import ChainValidator


class Field(object):

    def __init__(self, default=Undef, validators=(), **kw):
        for k, v in kw.items():
            setattr(self, k, v)
        self.default = default
        self.validators = validators
        # TODO: auto add validators and converters.

    def validate(self, value):
        return ChainValidator(*self.validators)(value)

    def set_default(self, obj):
        if self.default is Undef:
            return
        default = self.default
        if self.get_value(obj) is None:
            if isinstance(default, collections.Callable):
                try:
                    default(obj, self.name)
                except TypeError:
                    default = default()
            self.set_value(obj, default)

    def get_value(self, obj):
        return getattr(obj, self.name, None)

    def set_value(self, obj, value):
        return setattr(obj, self.name, value)
