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

    def __call__(self, model, fields):
        if model._meta.name in self:
            raise Exception("Already registered {}".format(
                model.__name__)
            )
        opts = model._meta
        self[opts.name] = fields
        opts.fields = collections.OrderedDict()

        rmap = {field.column: name for name, field in opts.declared_fields.items() if hasattr(field, 'column')}
        columns = {}
        for name in fields:
            if name in opts.declared_fields and hasattr(opts.declared_fields[name], 'column'):
                column = opts.declared_fields[name].column
            else:
                column = column
            for lang in self.get_languages():
                columns[self.translate_column(column, lang)] = column

        for column in opts.columns:
            field = opts.columns[column]
            if column in columns and not isinstance(field, TranslationField):
                original_column = columns[column]
                name = rmap.get(original_column, original_column)

                class NewTranslationField(TranslationField, field.__class__):
                    pass

                data = vars(opts.declared_fields[name]) if name in opts.declared_fields else {}
                data.update(vars(field))
                new_field = NewTranslationField(**data)
                self.add_field(model, new_field, name)
            elif field.name not in opts.fields:
                self.add_field(model, field, field.name)

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
