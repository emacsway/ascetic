import collections
from ascetic.mappers import Mapper

# We can't use TranslationRegistry, because Mapper can be inherited, and we need to fix hierarchy???


class TranslationMapper(Mapper):

    translated_fields = ()

    def create_fields(self, field_descriptions, declared_fields):
        return CreateFields(self, field_descriptions, declared_fields).compute()

    def create_translated_field(self, name, description, declared_fields=None):
        field = self.create_field(name, description, declared_fields)
        if name in self.translated_fields:
            column_descriptor = TranslationColumnDescriptor(self)
        else:
            column_descriptor = OriginalColumnDescriptor()
        field_factory = type("Translation{}".format(field.__class__.__name__), (field.__class__,), {
            'column': column_descriptor,
        })
        field = field_factory(**field.__dict__)
        return field

    def add_field(self, name, field):
        super(TranslationMapper, self).add_field(name, field)
        if name in self.translated_fields:
            for lang in self.get_languages():
                self.columns[self.translate_column(field.original_column, lang)] = field

    def make_identity_key(self, model, pk):
        return super(TranslationMapper, self).make_identity_key(model, pk) + (self.get_language(),)

    def translate_column(self, original_name, lang=None):
        return '{}_{}'.format(original_name, lang or self.get_language())

    def restore_column(self, translated_name):
        original_column, lang = translated_name.rsplit('_', 1)
        assert lang in self.get_languages()
        return original_column

    def get_language(self):
        raise NotImplementedError

    def get_languages(self):
        raise NotImplementedError


class CreateFields(object):

    def __init__(self, mapper, field_descriptions, declared_fields):
        self._mapper = mapper
        self._field_descriptions = field_descriptions
        self._declared_fields = declared_fields
        self._fields = collections.OrderedDict()
        self._translated_column_mapping = {}
        self._reverse_mapping = {field.column: name for name, field in self._declared_fields.items()
                                 if hasattr(field, 'column')}

    def compute(self):
        self._create_translated_column_mapping()
        self._create_fields()
        self._create_virtual_fields()
        return self._fields

    def _create_translated_column_mapping(self):
        for name in self._mapper.translated_fields:
            try:
                column = self._declared_fields[name].column
            except (KeyError, AttributeError):
                column = name
            for lang in self._mapper.get_languages():
                self._translated_column_mapping[self._mapper.translate_column(column, lang)] = column

    def _create_fields(self):
        for field_description in self._field_descriptions:
            name = self._get_field_name(field_description['column'])
            if name not in self._fields:
                self._fields[name] = self._mapper.create_translated_field(name, field_description, self._declared_fields)

    def _create_virtual_fields(self):
        for name, field in self._declared_fields.items():
            if name not in self._fields:
                self._fields[name] = self._mapper.create_field(name, {'virtual': True}, self._declared_fields)

    def _get_field_name(self, translated_column):
        original_column = self._translated_column_mapping.get(translated_column, translated_column)
        return self._reverse_mapping.get(original_column, original_column)


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
