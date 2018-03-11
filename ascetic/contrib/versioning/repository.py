from __future__ import absolute_import
import copy
import datetime
import hashlib

from ascetic.contrib.versioning.interfaces import IRevisionRepository


class DatabaseRevisionRepository(IRevisionRepository):
    _registry = None

    def __init__(self, mapper, registry, comparator, actual_serializer, known_serializers):
        """
        :type mapper: ascetic.mappers.Mapper
        :type registry: ascetic.contrib.versioning.interfaces.IRegistry
        :type comparator: ascetic.contrib.versioning.interfaces.IComparator
        :type actual_serializer: ascetic.contrib.versioning.interfaces.ISerializer
        :type known_serializers: list[ascetic.contrib.versioning.interfaces.ISerializer]
        """
        self._mapper = mapper
        self._registry = registry
        self._comparator = comparator
        self._actual_serializer = actual_serializer
        self._known_serializers = [actual_serializer] + known_serializers

    def commit(self, obj, stamp, **info):
        prev_stamp = self.version(obj).stamp
        prev_obj = self.object_version(obj, stamp=prev_stamp)
        # We have to save each revision of changeset, even empty
        # if self._comparator.is_equal(prev_obj, obj):
        #     return

        delta = self._comparator.create_delta(prev_obj, obj)
        serialized_delta = self._actual_serializer.dumps(delta)
        hash_ = hashlib.sha1(
            serialized_delta.encode("utf-8")
        ).hexdigest()
        rev = self._mapper.model(
            content_object=obj,
            stamp=stamp,
            hash=hash_,
            delta=serialized_delta,
            created_at=datetime.datetime.now(),
            **info
        )
        self._do_commit(rev)

        try:
            rev.save()
        except Exception:
            # New revision was added by concurent process
            raise

        return rev

    def _do_commit(self, rev):
        pass

    def object_version(self, obj, stamp=None):
        object_version = self._make_init_object_version(obj.__class__, obj)
        versions = self.versions(obj).where(self._mapper.sql_table.reverted == False)
        if stamp is not None:
            versions = versions.where((self._mapper.sql_table.stamp <= stamp))
        for version in versions:
            delta = self._deserialize(version.delta)
            self._comparator.apply_delta(object_version, delta)
        return object_version

    def versions(self, obj):
        t = self._mapper.sql_table
        return self._mapper.query.where(
            (t.content_type_id == obj) &
            (t.object_id == obj)
        ).order_by(
            t.stamp
        )

    def version(self, obj, stamp=None):
        t = self._mapper.sql_table
        q = self.versions(obj)
        if stamp is not None:
            q = q.where((t.stamp == stamp))
        return q[0]

    def _make_init_object_version(self, model, init_obj=None):
        object_accessor = self._registry.get_object_accessor(model)
        object_version = copy.deepcopy(init_obj) if init_obj else model()
        for field_name in self._registry.get_fields(model):
            object_accessor.set_value(object_version, field_name, None)
        return object_version

    def _deserialize(self, dump):
        """
        :type dump: string
        :rtype: collections.Mapping
        """
        for serializer in self._known_serializers:
            if serializer.is_acceptable(dump):
                return serializer.loads(dump)
        else:
            raise ValueError("Unknown version of the Serializer.")
