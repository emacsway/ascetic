import collections
from ..models import Field, mapper_registry

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
            class NewField(TranslationField, field.__class__):
                pass
            field = NewField(**field.__dict__)
        return field

    def create_fields(self, columns, declared_fields):
        fields = collections.OrderedDict()
        rmap = {field.column: name for name, field in declared_fields.items() if hasattr(field, 'column')}

        original_columns = {}
        for name in self.translated_fields:
            if name in declared_fields and hasattr(declared_fields[name], 'column'):
                field = declared_fields[name]
                column = field.original_column if isinstance(field, TranslationField) else field.column
            else:
                column = name
            for lang in self.get_languages():
                original_columns[self.translate_column(column, lang)] = column

        for data in columns:
            column_name = data['column']
            column_name = original_columns.get(column_name, column_name)
            name = rmap.get(column_name, column_name)
            if name in fields:
                continue
            fields[name] = self.create_translation_field(name, data, declared_fields)

        for name, field in declared_fields.items():
            if name not in fields:
                fields[name] = self.create_translation_field(name, {'virtual': True}, declared_fields)
        return fields

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


"""
class TranslationRegistry(dict):

    _singleton = None

    def __new__(cls, *args, **kwargs):
        if not TranslationRegistry._singleton:
            if cls is TranslationRegistry:
                raise Exception("Can not create instance of abstract class {}".format(cls))
            TranslationRegistry._singleton = super(TranslationRegistry, cls).__new__(cls, *args, **kwargs)
        return TranslationRegistry._singleton

    def __call__(self, mapper, translated_fields):
        if mapper.name in self:
            raise Exception("Already registered {}".format(
                mapper.name)
            )
        self[mapper.name] = translated_fields
        mapper.fields = collections.OrderedDict()

        rmap = {field.column: name for name, field in mapper.declared_fields.items() if hasattr(field, 'column')}
        original_columns = {}
        for name in translated_fields:
            if name in mapper.declared_fields and hasattr(mapper.declared_fields[name], 'column'):
                column = mapper.declared_fields[name].column
            else:
                column = name
            for lang in self.get_languages():
                original_columns[self.translate_column(column, lang)] = column

        for column in mapper.columns:
            original_column = original_columns[column]
            name = rmap.get(original_column, original_column)
            if name in mapper.fields:
                field = mapper.fields[name]
            else:
                field = mapper.columns[column]
                if column in original_columns and not isinstance(field, TranslationField):
                    class NewTranslationField(TranslationField, field.__class__):
                        pass

                    data = vars(mapper.declared_fields[name]) if name in mapper.declared_fields else {}
                    data.update(vars(field))
                    field = NewTranslationField(**data)
            self.add_field(mapper, column, name, field)

        mapper.pk = mapper._read_pk(mapper.db_table, mapper._using, mapper.columns)
        mapper.sql_table = mapper._create_sql_table()
        mapper.base_query = mapper._create_base_query()
        mapper.query = mapper._create_query()

    def add_field(self, mapper, column, name, field):
        field.name = name
        field._mapper = mapper
        mapper.fields[name] = field
        mapper.columns[column] = field
"""
