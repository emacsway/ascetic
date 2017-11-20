import collections, weakref
from ascetic import interfaces, utils


class Observable(interfaces.IObservable):
    def __init__(self, subject_accessor):
        self.get_subject = subject_accessor
        self._observers = collections.defaultdict(list)

    def attach(self, aspects, observer=None):
        """
        :type aspects: collections.Hashable or list[collections.Hashable]
        :type observer: callable
        :rtype: ascetic.interfaces.IDisposable
        """
        if observer is None:
            observer, aspects = aspects, None
        aspects = utils.to_tuple(aspects)
        for aspect in aspects:
            self._observers[aspect].append(observer)
        return Disposable(self, aspects, observer)

    def detach(self, aspects, observer):
        """
        :type aspects: collections.Hashable or list[collections.Hashable]
        :type observer: callable
        """
        if observer is None:
            observer, aspects = aspects, None
        aspects = utils.to_tuple(aspects)
        for aspect in aspects:
            self._observers[aspect].remove(observer)

    def notify(self, aspect, *args, **kwargs):
        """
        :type aspect: collections.Hashable
        """
        observers = self._observers[None] + self._observers[aspect]
        for observer in observers:
            observer(self.get_subject(), aspect, *args, **kwargs)

    def is_null(self):
        """
        :rtype: bool
        """
        return False


class DummyObservable(interfaces.IObservable):
    def attach(self, aspects, observer):
        """
        :type aspects: collections.Hashable or list[collections.Hashable]
        :type observer: callable
        :rtype: ascetic.interfaces.IDisposable
        """
        return Disposable(self, None, None)

    def detach(self, aspects, observer):
        """
        :type aspects: collections.Hashable or list[collections.Hashable]
        :type observer: callable
        """

    def notify(self, aspect, *args, **kwargs):
        """
        :type aspect: collections.Hashable
        """

    def is_null(self):
        """
        :rtype: bool
        """
        return True


class Disposable(interfaces.IDisposable):
    def __init__(self, observed, aspect, observer):
        self._observed = observed
        self._aspect = aspect
        self._observer = observer

    def dispose(self):
        self._observed.detach(self._aspect, self._observer)

    def __add__(self, other):
        return CompositeDisposable(self, other)


class CompositeDisposable(interfaces.IDisposable):
    def __init__(self, *delegates):
        self._delegates = list(delegates)

    def dispose(self):
        for delegate in self._delegates:
            delegate.dispose()

    def __add__(self, other):
        return CompositeDisposable(*(self._delegates + [other]))


def observe(subject, accessor_name='observed', factory=Observable):
    observable = factory(weakref.ref(subject))
    setattr(subject, accessor_name, lambda: observable)
    return subject
