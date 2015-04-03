import copy
import collections
from .. import models, signals

# Not testet yet!!! It's just a draft!!!


class TranslationDictMixIn(object):

    def __getitem__(self, key):
        try:
            return super(TranslationDictMixIn, self).__getitem__(key)
        except KeyError:
            return super(TranslationDictMixIn, self).__getitem__(TranslationRegistry.registry.translated_field(self.model, key))


class OriginalDictMixIn(object):

    def __getitem__(self, key):
        try:
            return super(TranslationDictMixIn, self).__getitem__(key)
        except KeyError:
            return super(TranslationDictMixIn, self).__getitem__(TranslationRegistry.registry.original_field(self.model, key))


class TranslationOrderedDict(TranslationDictMixIn, collections.OrderedDict):
    pass


class TranslationDict(TranslationDictMixIn, dict):
    pass


class OriginalOrderedDict(OriginalDictMixIn, collections.OrderedDict):
    pass


class OriginalDict(OriginalDictMixIn, dict):
    pass


class TranslationMixIn(models.Model):

    def __setattr__(self, name, value):
        reg = TranslationRegistry.registry
        translated_name = reg.translated_field(type(self), name)
        super(TranslationMixIn, self).__setattr__(translated_name, value)

    def __getattr__(self, name):
        reg = TranslationRegistry.registry
        translated_name = reg.translated_field(type(self), name)
        super(TranslationMixIn, self).__getattr__(translated_name)

    def _set_data(self, data):
        reg = TranslationRegistry.registry
        for column, value in data.items():
            name = self._meta.columns[column].name
            original_name = reg.original_field(type(self), name, only_current=True)
            if original_name != name and value is None:
                    for lang in reg.get_languages():
                        try_name = reg.translated_field(type(self), original_name, lang=lang)
                        try_column = type(self)._meta.fields[try_name].column
                        if data.get(try_column) is not None:
                            data[column] = data[try_column]
                            break
        return super(TranslationMixIn, self)._set_data(data)

    def _validate(self, exclude=frozenset(), fields=frozenset()):
        reg = TranslationRegistry.registry
        exclude = frozenset(reg.original_field(self.__class__, f) for f in exclude)
        fields = frozenset(reg.original_field(self.__class__, f) for f in fields)
        super(TranslationMixIn, self)._validate(exclude, fields)


class TranslationRegistry(dict):

    def __init__(self):
        if hasattr(TranslationRegistry, 'registry'):
            raise Exception("Already registered {}".format(
                type(TranslationRegistry.registry).__name__)
            )
        TranslationRegistry.registry = self
        self.connect_field_conversion()

    def __call__(self, model, fields):
        if model._meta.name in self:
            raise Exception("Already registered {}".format(
                model.__name__)
            )

        model.__bases__ = (TranslationMixIn, ) + model.__bases__
        self[model._meta.name] = d = {}
        for name in fields:
            d[name] = s = set()
            for lang in self.get_languages():
                translated_name = "{}_{}".format(name, lang)
                s.add(translated_name)
                model._meta.fields[translated_name].original_name = name

        model._meta.fields.__class__ = TranslationOrderedDict
        model._meta.fields.model = model
        model._meta.validations.__class__ = OriginalDict
        model._meta.validations.model = model

        for name, field in self.declared_fields.items():
            if hasattr(field, 'column') and field.column not in model._meta.columns:
                for lang in self.get_languages():
                    trans_column = "{}_{}".format(field.column, lang)
                    trans_field = model._meta.columns[trans_column]
                    new_field = copy.copy(field)
                    new_field.__dict__.update(trans_field.__dict__)
                    model._meta.add_field(new_field, name)

    def translated_field(self, model, field, lang=None):
        lang = lang or self.get_language()
        if field in self[model._meta.name]:
            return "{}_{}".format(field, lang)
        return field

    def original_field(self, model, field, lang=None, only_current=True):
        lang = lang or self.get_language()
        for key, values in self[model._meta.name].items():
            if field in values:
                if not only_current or field.rsplit('_', 1).pop() == lang:
                    return key
        return field

    def field_conversion_receiver(self, sender, result, field, model):
        result['field'] = self.translated_field(model, field)

    def connect_field_conversion(self):
        signals.field_conversion.connect(self.field_conversion_receiver)

    def get_language(self):
        raise NotImplementedError

    def get_languages(self, lang=None):
        raise NotImplementedError
