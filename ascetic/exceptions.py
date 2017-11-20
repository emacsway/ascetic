class OrmException(Exception):
    pass


class MapperNotRegistered(OrmException):
    pass


class ObjectDoesNotExist(OrmException):
    pass


class ValidationError(ValueError):
    pass
