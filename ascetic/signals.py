from __future__ import absolute_import
from weakref import WeakKeyDictionary, WeakValueDictionary

__all__ = ('Signal', 'pre_save', 'post_save', 'pre_delete', 'post_delete',
           'pre_init', 'post_init', 'class_prepared', 'field_conversion')


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
                self._weak_cache.discard(receiver or self._receivers[sender][receiver_id])
                del self._receivers[sender][receiver_id]
            except KeyError:
                pass
        else:
            raise ValueError('a receiver or a receiver_id must be provided')

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


def connect(signal, sender=None, weak=True, receiver_id=None):
    def decorator(fn):
        signal.connect(fn, sender, weak, receiver_id)
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
