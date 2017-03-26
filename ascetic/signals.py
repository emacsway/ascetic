from __future__ import absolute_import
from weakref import WeakKeyDictionary, WeakValueDictionary

__all__ = ('Signal', 'pre_save', 'post_save', 'pre_delete', 'post_delete',
           'pre_init', 'post_init', 'class_prepared', 'field_mangling', 'column_mangling', )


class UndefinedSender(object):
    pass

undefined_sender = UndefinedSender()


class Signal(object):

    def __init__(self):
        self._flush()

    def _flush(self):
        self._receivers = WeakKeyDictionary()
        self._weak_cache = set()

    def connect(self, receiver, sender=None, weak=True, receiver_id=None):
        if sender is None:
            sender = undefined_sender
        if not weak:
            self._weak_cache.add(receiver)
        if receiver_id is None:
            receiver_id = self._make_id(receiver)
        if sender not in self._receivers:
            self._receivers[sender] = WeakValueDictionary()
        self._receivers[sender][receiver_id] = receiver

    @staticmethod
    def _make_id(target):
        if hasattr(target, '__func__'):
            return (type(target), id(target.__self__), id(target.__func__))
        return (type(target), id(target))

    def disconnect(self, receiver=None, sender=None, receiver_id=None):
        if sender is None:
            sender = undefined_sender
        if receiver_id is None:
            receiver_id = self._make_id(receiver)
        if receiver_id:
            try:
                self._weak_cache.discard(receiver or self._receivers[sender][receiver_id])
                del self._receivers[sender][receiver_id]
                if not self._receivers[sender]:
                    del self._receivers[sender]
            except KeyError:
                pass
        else:
            raise ValueError('a receiver or a receiver_id must be provided')

    def send(self, sender, *args, **kwargs):
        if sender is None:
            sender = undefined_sender
        responses = []
        if sender in self._receivers:
            for receiver in self._receivers[sender].values():
                responses.append((receiver, receiver(sender, *args, **kwargs)))
        if sender is not undefined_sender:
            responses += self.send(undefined_sender, *args, **kwargs)
        return responses


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
field_mangling = Signal()
column_mangling = Signal()
