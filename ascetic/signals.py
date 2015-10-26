from __future__ import absolute_import
import random
from weakref import WeakKeyDictionary, WeakValueDictionary

__all__ = ('Signal', 'pre_save', 'post_save', 'pre_delete', 'post_delete',
           'pre_init', 'post_init', 'class_prepared', 'field_conversion')


class Signal(object):

    _clearing_frequency = 0.3

    def __init__(self):
        self._flush()

    def _flush(self):
        self._receivers = WeakKeyDictionary()
        # Different senders can have the same hash-value.
        # Hash can be primary key or something other.
        # Eventually, sender can be non-hashable.
        # So, we can't use sender as key in WeakKeyDictionary()
        # We store sender in WeakValueDictionary() to track sender_id
        self._senders = WeakValueDictionary()
        self._weak_cache = set()

    def connect(self, receiver, sender=None, weak=True, receiver_id=None):
        if not weak:
            self._weak_cache.add(receiver)
        if receiver_id is None:
            receiver_id = self._make_id(receiver)
        sender_id = self._make_id(sender)
        if sender_id not in self._receivers:
            self._receivers[sender_id] = WeakValueDictionary()
            # self._senders holds link to hashable key of WeakKeyDictionary
            self._senders[sender_id] = sender
        self._receivers[sender_id][receiver_id] = receiver

    @staticmethod
    def _make_id(target):
        if hasattr(target, '__func__'):
            return (type(target), id(target.__self__), id(target.__func__))
        return (type(target), id(target))

    def disconnect(self, receiver=None, sender=None, receiver_id=None):
        if receiver_id is None:
            receiver_id = self._make_id(receiver)
        if receiver_id:
            try:
                sender_id = self._make_id(sender)
                self._weak_cache.discard(receiver or self._receivers[sender_id][receiver_id])
                del self._receivers[sender_id][receiver_id]
                if not self._receivers[sender_id]:
                    del self._receivers[sender_id]
                    del self._senders[sender_id]
            except KeyError:
                pass
        else:
            raise ValueError('a receiver or a receiver_id must be provided')

    def send(self, sender, *args, **kwargs):
        responses = []
        sender_id = self._make_id(sender)
        if sender_id in self._receivers:
            for receiver in self._receivers[sender_id].values():
                responses.append((receiver, receiver(sender, *args, **kwargs)))
        if sender is not None:
            responses += self.send(None, *args, **kwargs)
        return responses

    def _clear_receivers(self):
        # WeakKeyDictionary vs dict with clearing
        # Dead code. Kill me if all works well.
        if random.random() < self._clearing_frequency:
            for sender_id, receivers in list(self._receivers.items()):
                if not receivers or sender_id not in self._senders:
                    del self._receivers[sender_id]


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
