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


class TranslationMapper(object):

    translated_fields = ()

    def create_translation_field(self, name, data, declared_fields=None):
        field = super(TranslationMapper, self).create_field(name, data, declared_fields)
        if name in self.translated_fields and not isinstance(field, TranslationField):
            NewField = type("Translation{}".format(field.__class__.__name__), (TranslationField, field.__class__), {})
            field = NewField(**field.__dict__)
            # field.__class__ = NewField
        return field

    def create_fields(self, columns, declared_fields):
        fields = collections.OrderedDict()
        rmap = {field.column: name for name, field in declared_fields.items() if hasattr(field, 'column')}
        translated_columns_map = self._create_translated_columns_map(declared_fields)

        for data in columns:
            column_name = data['column']
            column_name = translated_columns_map.get(column_name, column_name)
            name = rmap.get(column_name, column_name)
            if name in fields:
                continue
            fields[name] = self.create_translation_field(name, data, declared_fields)

        for name, field in declared_fields.items():
            if name not in fields:
                fields[name] = self.create_translation_field(name, {'virtual': True}, declared_fields)
        return fields

    def _create_translated_columns_map(self, declared_fields):
        translated_columns_map = {}
        for name in self.translated_fields:
            if name in declared_fields and hasattr(declared_fields[name], 'column'):
                field = declared_fields[name]
                column = field.original_column if isinstance(field, TranslationField) else field.column
            else:
                column = name
            for lang in self.get_languages():
                translated_columns_map[self.translate_column(column, lang)] = column
        return translated_columns_map

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
