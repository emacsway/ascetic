import collections
from .. import models

# Not testet yet!!! It's just a draft!!!


class TranslationColumnDescriptor(object):

    def __get__(self, instance, owner):
        if not instance:
            return self
        return TranslationRegistry().translate_column(instance._column)

    def __set__(self, instance, value):
        instance._column = value


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

    def __call__(self, model, fields):
        if model._meta.name in self:
            raise Exception("Already registered {}".format(
                model.__name__)
            )
        opts = model._meta
        self[opts.name] = fields
        opts.fields = collections.OrderedDict()

        columns = set()
        for field in fields:
            if field in self.map:
                columns.add(self.map[field])
            else:
                columns.add(field)

        expected_translated_field_names = {}
        for field in fields:
            for lang in self.get_languages():
                expected_translated_field_names[self.translate_column(field, lang)] = field

        # TODO: lost names from map. Kill map, create declared field on fly?
        for name, field in opts.declared_fields.copy().items():
            if field.name in fields and field.column not in opts.columns:   # Declared with specific column, i.e. lost mapping

                class NewTranslationField(TranslationField, field.__class__):
                    pass

                new_field = NewTranslationField(**vars(field))
                real_field = opts.columns[new_field.column]
                real_data = vars(real_field)
                real_data.pop('name')
                real_data.pop('column')
                new_field.__dict__.update(real_data)
                opts.declared_fields[name] = new_field
                self.add_field(model, new_field, name)  # Rewrite field name to maped name

        for column, field in opts.columns:
            if field.name in expected_translated_field_names and not isinstance(field, TranslationField):
                original_field_name = expected_translated_field_names[field.name]

                class NewTranslationField(TranslationField, field.__class__):
                    pass

                data = vars(field)
                data['column'] = column.rsplit('_', 1)[0]
                new_field = NewTranslationField(**data)
                if original_field_name in opts.declared_fields:  # Declared without specific column
                    new_field = opts.declared_fields[original_field_name]
                self.add_field(model, new_field, original_field_name)

    def add_field(self, model, field, name):
        field.name = name
        field.model = model
        if getattr(field, b'validators', None):
            model._meta.validations[name] = field.validators
        model._meta.fields[name] = field
        if isinstance(field, TranslationField):
            for lang in self.get_languages():
                model._meta.columns[self.translate_column(field.original_column, lang)] = field
        else:
            model._meta.columns[field.column] = field

    def translate_column(self, name, lang=None):
        return '{}_{}'.format(name, lang or self.get_language())

    def get_language(self):
        raise NotImplementedError

    def get_languages(self, lang=None):
        raise NotImplementedError
