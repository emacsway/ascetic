"""
Extracted from https://github.com/coleifer/peewee
"""
from __future__ import absolute_import, unicode_literals


class Signal(object):
    def __init__(self):
        self._flush()

    def connect(self, receiver, name=None, sender=None):
        name = name or receiver.__name__
        if name not in self._receivers:
            self._receivers[name] = (receiver, sender)
            self._receiver_list.append(name)
        else:
            raise ValueError('receiver named {0} already connected'.format(name))

    def disconnect(self, receiver=None, name=None):
        if receiver:
            name = receiver.__name__
        if name:
            del self._receivers[name]
            self._receiver_list.remove(name)
        else:
            raise ValueError('a receiver or a name must be provided')

    def send(self, sender, *args, **kwargs):
        responses = []
        for name in self._receiver_list:
            r, s = self._receivers[name]
            if s is None or sender is s:
                responses.append((r, r(sender, *args, **kwargs)))
        return responses

    def _flush(self):
        self._receivers = {}
        self._receiver_list = []


def connect(signal, name=None, sender=None):
    def decorator(fn):
        signal.connect(fn, name, sender)
        return fn
    return decorator


pre_save = Signal()
post_save = Signal()
pre_delete = Signal()
post_delete = Signal()
pre_init = Signal()
post_init = Signal()
class_prepared = Signal()
field_conversion = Signal()
