from .. import signals

# Not testet yet!!!


def required(obj, key, val):
    reg = TranslationRegistry.registry
    current_key = reg.translated_field(reg.original_field(key))
    return key == current_key and val not in [None, '']


class TranslationMixIn(object):

    def __setattr__(self, name, value):
        reg = TranslationRegistry.registry
        translated_name = reg.translated_field(type(self), name)
        if translated_name != name:
            super(TranslationMixIn, self).__setattr__(translated_name, value)
        else:
            original_name = reg.original_field(type(self), name, only_current=True)
            if original_name != name:
                super(TranslationMixIn, self).__setattr__(original_name, value)
        super(TranslationMixIn, self).__setattr__(name, value)

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

    def translated_field(self, model, field, lang=None):
        lang = lang or self.get_language()
        if field in self[model._meta.name]:
            return "{}_{}".format(field, lang)
        return field

    def original_field(self, model, field, lang=None, only_current=True):
        lang = lang or self.get_language()
        for key, values in self[model._meta.name]:
            if field in values:
                if not only_current or field.rsplit('_', 1).pop() == lang:
                    return key
        return field

    def field_conversion_receiver(self, sender, result, field, model):
        trans_field = self.translated_field(model, field)
        if trans_field != field:
            result['field'] = trans_field

    def connect_field_conversion(self):
        signals.field_conversion.connect(self.field_conversion_receiver)

    def get_language(self):
        raise NotImplementedError

    def get_languages(self, lang=None):
        raise NotImplementedError
