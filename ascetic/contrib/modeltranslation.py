import collections
from ..models import Field

# We can't use TranslationRegistry, because Mapper can be inherited, and we need to fix hierarchy???


class TranslationColumnDescriptor(object):

    def __get__(self, instance, owner):
        if not instance:
            return self
        return instance._mapper.translate_column(instance.original_column)

    def __set__(self, instance, value):
        instance.original_column = value.rsplit('_', 1)[0]


class TranslationField(Field):
    column = TranslationColumnDescriptor()


class CreateFields(object):

    def __init__(self, mapper, columns, declared_fields):
        self._mapper = mapper
        self._columns = columns
        self._declared_fields = declared_fields
        self._fields = collections.OrderedDict()
        self._reversed_map = {
            field.column: name for name, field in self._declared_fields.items() if hasattr(field, 'column')
        }  # That's why we can't rename currently create_translation_field() to create_field()

    def compute(self):
        self._create_translated_columns_map()
        self._create_fields()
        self._create_virtual_fields()
        return self._fields

    def _create_translated_columns_map(self):
        self._translated_columns_map = {}
        for name in self._mapper.translated_fields:
            if name in self._declared_fields and hasattr(self._declared_fields[name], 'column'):
                field = self._declared_fields[name]
                column = field.original_column if isinstance(field, TranslationField) else field.column
            else:
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
            self._fields[name] = self._mapper.create_translation_field(name, data, self._declared_fields)

    def _create_virtual_fields(self):
        for name, field in self._declared_fields.items():
            if name not in self._fields:
                self._fields[name] = self._mapper.create_translation_field(name, {'virtual': True}, self._declared_fields)


class TranslationMapper(object):

    translated_fields = ()

    def create_translation_field(self, name, data, declared_fields=None):
        field = super(TranslationMapper, self).create_field(name, data, declared_fields)
        if name in self.translated_fields and not isinstance(field, TranslationField):
            NewField = type("Translation{}".format(field.__class__.__name__), (TranslationField, field.__class__), {})
            field = NewField(**field.__dict__)
        return field

    def create_fields(self, columns, declared_fields):
        return CreateFields(self, columns, declared_fields).compute()

    def add_field(self, name, field):
        field.name = name
        field._mapper = self
        self.fields[name] = field
        if isinstance(field, TranslationField):
            for lang in self.get_languages():
                self.columns[self.translate_column(field.original_column, lang)] = field
        else:
            self.columns[field.column] = field

    def _make_identity_key(self, model, pk):
        return super(TranslationMapper, self)._make_identity_key(model, pk) + (self.get_language(),)

    def translate_column(self, name, lang=None):
        return '{}_{}'.format(name, lang or self.get_language())

    def get_language(self):
        raise NotImplementedError

    def get_languages(self):
        raise NotImplementedError
