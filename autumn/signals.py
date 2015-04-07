"""
Extracted from https://github.com/coleifer/peewee
"""
from __future__ import absolute_import


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

signals = {}
for name in ['pre_save', 'post_save', 'pre_delete', 'post_delete',
             'pre_init', 'post_init', 'class_prepared', 'field_conversion']:
    signals[name] = Signal()

globals().update(signals)


def send_signal(signal, *a, **kw):
    """Send signal abstract handler.

    You can to override it by settings.SIGNAL_SEND_HANDLER
    For example, you can use one from next event systems:
    https://github.com/jesusabdullah/pyee
    https://bitbucket.org/jek/blinker
    https://launchpad.net/pydispatcher
    https://github.com/theojulienne/PySignals
    https://github.com/olivierverdier/dispatch
    and others.
    """
    return signals[signal].send(*a, **kw)

from . import settings
from .utils import resolve
if settings.SIGNAL_SENDER != 'autumn.signals.send_signal':
    send_signal = resolve(settings.SIGNAL_SENDER)
