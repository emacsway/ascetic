from ascetic.contrib.versioning.exceptions import AlreadyRegistered, NotRegistered
from ascetic.contrib.versioning.interfaces import IRegistry


class Registry(IRegistry):
    def __init__(self):
        self._fields_mapping = dict()
        self._object_accessor_mapping = dict()

    def register(self, model, fields, object_accessor):
        if model in self._fields_mapping:
            raise AlreadyRegistered("Already registered {0}".format(model.__name__))
        self._fields_mapping[model] = fields
        self._object_accessor_mapping[model] = fields

    def unregister(self, model):
        """
        :type model: object
        """
        del self._fields_mapping[model]
        del self._object_accessor_mapping[model]

    def get_fields(self, model):
        try:
            return self._fields_mapping[model]
        except KeyError:
            raise NotRegistered('"{0}" is not registerd'.format(model))

    def get_object_accessor(self, model):
        try:
            return self._object_accessor_mapping[model]
        except KeyError:
            raise NotRegistered('"{0}" is not registerd'.format(model))
