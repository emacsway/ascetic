import collections
from .. import models

# Not testet yet!!! It's just a draft!!!


class TranslationColumnDescriptor(object):

    def __get__(self, instance, owner):
        if not instance:
            return self
        return TranslationRegistry().translate_column(instance._column)

    def __set__(self, instance, value):
        instance._column = value.rsplit('_', 1)[0]


class OriginalColumnDescriptor(object):

    def __get__(self, instance, owner):
        if not instance:
            return self
        return instance._column

    def __set__(self, instance, value):
        instance._column = value


class TranslationField(models.Field):
    column = TranslationColumnDescriptor()
    original_column = OriginalColumnDescriptor()


class TranslationRegistry(dict):

    _singleton = None

    def __new__(cls, *args, **kwargs):
        if not TranslationRegistry._singleton:
            if cls is TranslationRegistry:
                raise Exception("Can not create instance of abstract class {}".format(cls))
            TranslationRegistry._singleton = super(TranslationRegistry, cls).__new__(cls, *args, **kwargs)
        return TranslationRegistry._singleton

    def __call__(self, gateway, translate_fields):
        if gateway.name in self:
            raise Exception("Already registered {}".format(
                gateway.name)
            )
        self[gateway.name] = translate_fields
        gateway.fields = collections.OrderedDict()

        rmap = {field.column: name for name, field in gateway.declared_fields.items() if hasattr(field, 'column')}
        original_columns = {}
        for name in translate_fields:
            if name in gateway.declared_fields and hasattr(gateway.declared_fields[name], 'column'):
                column = gateway.declared_fields[name].column
            else:
                column = column
            for lang in self.get_languages():
                original_columns[self.translate_column(column, lang)] = column

        for column in gateway.columns:
            original_column = original_columns[column]
            name = rmap.get(original_column, original_column)
            if name in gateway.fields:
                field = gateway.fields[name]
            else:
                field = gateway.columns[column]
                if column in original_columns and not isinstance(field, TranslationField):
                    class NewTranslationField(TranslationField, field.__class__):
                        pass

                    data = vars(gateway.declared_fields[name]) if name in gateway.declared_fields else {}
                    data.update(vars(field))
                    field = NewTranslationField(**data)
            self.add_field(gateway, column, name, field)

        gateway.pk = gateway._read_pk(gateway.db_table, gateway._using, gateway.columns)
        gateway.sql_table = gateway._create_sql_table()
        gateway.base_query = gateway._create_base_query()
        gateway.query = gateway._create_query()

    def add_field(self, gateway, column, name, field):
        field.name = name
        field._gateway = gateway
        gateway.fields[name] = field
        gateway.columns[column] = field

    def translate_column(self, name, lang=None):
        return '{}_{}'.format(name, lang or self.get_language())

    def get_language(self):
        raise NotImplementedError

    def get_languages(self, lang=None):
        raise NotImplementedError
