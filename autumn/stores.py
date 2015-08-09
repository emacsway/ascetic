import operator
from . import signals
from .models import ForeignKey, OneToOne, OneToMany, IdentityMap, registry, to_tuple


class Store(object):

    def __init__(self, alias, database):
        self._alias = alias
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
        if obj in queue:
            return
        queue.append(obj)
        if obj.__class__ in self._dirty and obj in self._dirty[obj.__class__]:
            self._dirty[obj.__class__].pop()

        for key, rel in obj.__class__._gateway.relations.items():
            if isinstance(rel, OneToMany):
                for child in getattr(obj, key).iterator():
                    rel.on_delete(obj, child, rel, self._alias)
            elif isinstance(rel, OneToOne):
                child = getattr(obj, key)
                rel.on_delete(obj, child, rel, self._alias)
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
                                removed_queue.append(obj)
                                queries.append(self._database.compile(model._gateway.delete_query(obj)))
                        if model in dirty:
                            dirty[model].sort(key=lambda x: x.pk)
                            model_pk = to_tuple(self.pk)
                            for obj in dirty[model]:
                                signals.send_signal(signal='pre_save', sender=self.model, instance=obj, using=self._alias)
                                saved_queue.append(obj)
                                if obj._new_record:
                                    queries.append(self._database.compile(model._gateway.insert_query(obj)))
                                    auto_pk = not all(getattr(obj, k, False) for k in model_pk)
                                    if auto_pk:
                                        queries.append("INSERT INTO autumn_pk_log VALUES(lastval())")  # SELECT LAST_INSERT_ID()
                                        pk_queue.append(obj)
                                else:
                                    queries.append(self._database.compile(model._gateway.update_query(obj)))

        queries.append("SELECT pk FROM autumn_pk_log")
        # Bulk execute

        cursor = self._database.execute(*((';'.join(q), tuple(reduce(operator.add, p))) for q, p in zip(*queries)))
        for obj, pk in zip(pk_queue, cursor.fetchall()):
            obj.__class__._gateway.set_pk(pk)

        for obj in removed_queue:
            signals.send_signal(signal='post_delete', sender=self.model, instance=obj, using=self._alias)
            IdentityMap(self._alias).remove((obj.__class__, to_tuple(obj.__class__._gateway.get_pk(obj))))

        for obj in saved_queue:
            is_new = obj._new_record
            signals.send_signal(signal='post_delete', sender=self.model, instance=obj, using=self._alias, created=is_new)
            IdentityMap(self._alias).add((obj.__class__, to_tuple(obj.__class__._gateway.get_pk(obj))), obj)

        if self._removed or self._dirty:
            # Store was filled by signals
            self.bulk_flush()
