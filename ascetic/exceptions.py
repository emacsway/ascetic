class OrmException(Exception):
    pass


class ModelNotRegistered(OrmException):
    pass


class MapperNotRegistered(OrmException):
    pass


class ObjectDoesNotExist(OrmException):
    pass


class ValidationError(ValueError):
    pass
