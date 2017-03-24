import collections
from ascetic.mappers import Mapper

# We can't use TranslationRegistry, because Mapper can be inherited, and we need to fix hierarchy???


class TranslationMapper(Mapper):

    translated_fields = ()

    def create_field(self, name, data, declared_fields=None):
        field = super(TranslationMapper, self).create_field(name, data, declared_fields)
        if name in self.translated_fields:
            column_descriptor = TranslationColumnDescriptor(self)
        else:
            column_descriptor = OriginalColumnDescriptor()
        field_factory = type("Translation{}".format(field.__class__.__name__), (field.__class__,), {
            'column': column_descriptor,
        })
        field = field_factory(**field.__dict__)
        return field

    def create_fields(self, columns, declared_fields):
        return CreateFields(self, columns, declared_fields).compute()

    def add_field(self, name, field):
        super(TranslationMapper, self).add_field(name, field)
        if name in self.translated_fields:
            for lang in self.get_languages():
                self.columns[self.translate_column(field.original_column, lang)] = field

    def make_identity_key(self, model, pk):
        return super(TranslationMapper, self).make_identity_key(model, pk) + (self.get_language(),)

    def translate_column(self, name, lang=None):
        return '{}_{}'.format(name, lang or self.get_language())

    def restore_column(self, name):
        try:
            original_column, lang = name.rsplit('_', 1)
        except ValueError:
            pass
        else:
            if lang in self.get_languages():
                return original_column
        return name

    def get_language(self):
        raise NotImplementedError

    def get_languages(self):
        raise NotImplementedError


class CreateFields(object):

    def __init__(self, mapper, columns, declared_fields):
        self._mapper = mapper
        self._columns = columns
        self._declared_fields = declared_fields
        self._fields = collections.OrderedDict()
        self._reversed_map = {
            field.original_column: name
            for name, field in self._declared_fields.items()
            if hasattr(field, 'original_column')
        }

    def compute(self):
        self._create_translated_column_map()
        self._create_fields()
        self._create_virtual_fields()
        return self._fields

    def _create_translated_column_map(self):
        self._translated_columns_map = {}
        for name in self._mapper.translated_fields:
            try:
                column = self._declared_fields[name].original_column
            except (KeyError, AttributeError):
                column = name
            for lang in self._mapper.get_languages():
                self._translated_columns_map[self._mapper.translate_column(column, lang)] = column

    def _create_fields(self):
        for data in self._columns:
            column_name = data['column']
            column_name = self._translated_columns_map.get(column_name, column_name)
            name = self._reversed_map.get(column_name, column_name)
            if name in self._fields:
                continue
            self._fields[name] = self._mapper.create_field(name, data, self._declared_fields)

    def _create_virtual_fields(self):
        for name, field in self._declared_fields.items():
            if name not in self._fields:
                self._fields[name] = self._mapper.create_field(name, {'virtual': True}, self._declared_fields)


class TranslationColumnDescriptor(object):

    def __init__(self, mapper):
        self._mapper = mapper

    def __get__(self, instance, owner):
        if not instance:
            return self
        return self._mapper.translate_column(instance.original_column)

    def __set__(self, instance, value):
        instance.original_column = self._mapper.restore_column(value)


class OriginalColumnDescriptor(object):

    def __get__(self, instance, owner):
        if not instance:
            return self
        return instance.original_column

    def __set__(self, instance, value):
        instance.original_column = value
