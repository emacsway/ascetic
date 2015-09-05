from __future__ import absolute_import
from weakref import WeakKeyDictionary, WeakValueDictionary


class Signal(object):
    def __init__(self):
        self._flush()

    @staticmethod
    def _make_id(target):
        if hasattr(target, '__func__'):
            return (id(target.__self__), id(target.__func__))
        return id(target)

    def connect(self, receiver, sender=None, weak=True, receiver_id=None):
        if not weak:
            self._weak_cache.add(receiver)
        if receiver_id is None:
            receiver_id = self._make_id(receiver)
        if sender not in self._receivers:
            self._receivers[sender] = WeakValueDictionary()
        self._receivers[sender][receiver_id] = receiver

    def disconnect(self, receiver=None, sender=None, receiver_id=None):
        if receiver_id is None:
            receiver_id = self._make_id(receiver)
        if receiver_id:
            try:
                del self._receivers[sender][receiver_id]
            except KeyError:
                pass
            self._weak_cache.discard(receiver)
        else:
            raise ValueError('a receiver or a name must be provided')

    def send(self, sender, *args, **kwargs):
        responses = []
        if sender in self._receivers:
            receivers = list(self._receivers[sender].values())
            if sender is not None:
                receivers += list(self._receivers[None].values())
            for receiver in receivers:
                responses.append((receiver, receiver(sender, *args, **kwargs)))
        return responses

    def _flush(self):
        self._receivers = WeakKeyDictionary()
        self._weak_cache = set()


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
if settings.SIGNAL_SENDER != 'ascetic.signals.send_signal':
    send_signal = resolve(settings.SIGNAL_SENDER)
