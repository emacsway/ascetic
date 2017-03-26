import copy
import collections
from ascetic.mappers import Mapper
from ascetic.signals import column_mangling
from ascetic.utils import cached_property

# We can't use TranslationRegistry, because Mapper can be inherited, and we need to fix hierarchy???


class TranslationMapper(Mapper):

    translated_fields = ()

    @cached_property
    def translated_columns(self):
        translated_columns = []
        for name in self.translated_fields:
            try:
                translated_columns.append(self.declared_fields[name].column)
            except (KeyError, AttributeError):
                translated_columns.append(name)
        return tuple(translated_columns)

    def create_fields(self, field_descriptions, declared_fields):
        translated_column_mapping = {}
        for name in self.translated_fields:
            try:
                column = declared_fields[name].column
            except (KeyError, AttributeError):
                column = name
            for lang in self.get_languages():
                translated_column_mapping[self.translate_column(column, lang)] = column

        field_descriptions = copy.deepcopy(field_descriptions)
        for field_description in field_descriptions:
            translated_column = field_description['column']
            original_column = translated_column_mapping.get(translated_column, translated_column)
            field_description['column'] = original_column

        return super(TranslationMapper, self).create_fields(field_descriptions, declared_fields)

    def add_field(self, name, field):
        super(TranslationMapper, self).add_field(name, field)
        if name in self.translated_fields:
            for lang in self.get_languages():
                self.columns[self.translate_column(field.column, lang)] = field

    def make_identity_key(self, model, pk):
        return super(TranslationMapper, self).make_identity_key(model, pk) + (self.get_language(),)

    def _do_prepare_model(self, model):

        def mangle_column(sender, column, mapper):
            if column in mapper.translated_columns:
                return (0, mapper.translate_column(column))

        column_mangling.connect(mangle_column, sender=self.sql_table, weak=False)
        super(TranslationMapper, self)._do_prepare_model(self.model)

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
