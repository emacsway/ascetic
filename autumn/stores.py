import operator
from . import signals
from .models import ForeignKey, OneToOne, registry, to_tuple


class Store(object):

    def __init__(self, alias, database):
        self._dirty = {}
        self._removed = {}
        self._database = database

    def _resolve_dependencies(self, model):
        queue = [model]
        for rel in model._gateway.bound_relations:
            if isinstance(rel, (ForeignKey, OneToOne)):
                if rel.rel_model not in queue:
                    queue.extend(rel.rel_model)
        return reversed(queue)

    def save(self, obj):
        queue = self._dirty.setdefault(obj.__class__, [])
        if obj not in queue:
            queue.append(obj)
        return self

    def remove(self, obj):
        queue = self._removed.setdefault(obj.__class__, [])
        if obj not in queue:
            queue.append(obj)
        if obj.__class__ in self._dirty and obj in self._dirty[obj.__class__]:
            self._dirty[obj.__class__].pop()
        return self

    def bulk_flush(self):
        visited = []
        queries = []
        removed = self._removed
        self._removed = {}
        dirty = self._dirty
        self._dirty = {}
        removed_queue = []
        saved_queue = []
        pk_queue = []
        # TODO: use compile for whole query
        queries.append("""
            DROP TABLE IF EXISTS autumn_pk_log;
            CREATE TEMPORARY TABLE IF NOT EXISTS autumn_pk_log (pk integer NOT NULL)
        """)
        for proposed_model in registry.values():
            if proposed_model in dirty or proposed_model in removed:
                for model in self._resolve_dependencies(proposed_model):
                    if model not in visited:
                        visited.append(model)
                        if model in removed:
                            for obj in removed[model]:
                                signals.send_signal(signal='pre_delete', sender=self.model, instance=obj, using=self._alias)
                                queries.append(self._get_delete_query())
                                removed_queue.append(obj)
                        if model in dirty:
                            dirty[model].sort(key=lambda x: x.pk)
                            model_pk = to_tuple(self.pk)
                            for obj in dirty[model]:
                                signals.send_signal(signal='pre_save', sender=self.model, instance=obj, using=self._alias)
                                queries.append(self._get_save_query(obj))
                                saved_queue.append(obj)
                                if obj._new_record:
                                    auto_pk = not all(getattr(obj, k, False) for k in model_pk)
                                    if auto_pk:
                                        queries.append("INSERT INTO autumn_pk_log VALUES(lastval())")  # SELECT LAST_INSERT_ID()
                                        pk_queue.append(obj)

        queries.append("SELECT pk FROM autumn_pk_log")
        # Bulk execute

        cursor = self._database.execute(*((';'.join(q), tuple(reduce(operator.add, p))) for q, p in zip(*queries)))

        for obj in removed_queue:
            signals.send_signal(signal='post_delete', sender=self.model, instance=obj, using=self._alias)

        for obj in saved_queue:
            is_new = obj._new_record
            # TODO: set pk
            for pk in cursor.fetchall():
                obj.pk = pk

            signals.send_signal(signal='post_delete', sender=self.model, instance=obj, using=self._alias, created=is_new)

        if self._removed or self._dirty:
            # Store was filled by signals
            self.bulk_flush()

    def _get_save_query(self, obj):
        pass

    def _get_delete_query(self, obj):
        pass
