from __future__ import absolute_import, unicode_literals
import sys
import copy
import base64
# import json
import hashlib
from datetime import datetime
from ..models import mapper_registry

# Under construction!!! Not testet yet!!!

try:
    import cPickle as pickle
except ImportError:
    import pickle

from difflib import SequenceMatcher

if sys.version_info > (3, ):
    from .vendor.diff_match_patch.python3.diff_match_patch import diff_match_patch
else:
    from .vendor.diff_match_patch.python2.diff_match_patch import diff_match_patch

"""
BEGIN;
CREATE TABLE "versioning_revision" (
    "id" serial NOT NULL PRIMARY KEY,
    "object_id" varchar(255) NOT NULL,
    "content_type_id" integer NOT NULL REFERENCES "django_content_type" ("id") DEFERRABLE INITIALLY DEFERRED,
    "revision" integer NOT NULL,
    "reverted" boolean NOT NULL,
    "sha1" varchar(40) NOT NULL,
    "delta" text NOT NULL,
    "created_at" timestamp with time zone NOT NULL,
    "comment" varchar(255) NOT NULL,
    "editor_id" integer REFERENCES "auth_user" ("id") DEFERRABLE INITIALLY DEFERRED,
    "editor_ip" inet,
    UNIQUE ("object_id", "content_type_id", "revision")
)
;
CREATE INDEX "versioning_revision_object_id" ON "versioning_revision" ("object_id");
CREATE INDEX "versioning_revision_object_id_like" ON "versioning_revision" ("object_id" varchar_pattern_ops);
CREATE INDEX "versioning_revision_content_type_id" ON "versioning_revision" ("content_type_id");
CREATE INDEX "versioning_revision_revision" ON "versioning_revision" ("revision");
CREATE INDEX "versioning_revision_reverted" ON "versioning_revision" ("reverted");
CREATE INDEX "versioning_revision_sha1" ON "versioning_revision" ("sha1");
CREATE INDEX "versioning_revision_sha1_like" ON "versioning_revision" ("sha1" varchar_pattern_ops);
CREATE INDEX "versioning_revision_created_at" ON "versioning_revision" ("created_at");
CREATE INDEX "versioning_revision_editor_id" ON "versioning_revision" ("editor_id");

COMMIT;
"""


try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)

PICKLED_MARKER = 'pickled'
STR_MARKER = 'str'

dmp = diff_match_patch()


class AlreadyRegistered(Exception):
    pass


class Registry(dict):

    _singleton = None

    def __new__(cls, *args, **kwargs):
        if not Registry._singleton:
            Registry._singleton = super(Registry, cls).__new__(cls, *args, **kwargs)
        return Registry._singleton

    def __call__(self, model, fields):
        if model in self:
            raise AlreadyRegistered("Already registered {}".format(
                model.__name__)
            )


class IRepository(object):

    def commit(self, obj):
        raise NotImplementedError

    def versions(self, obj):
        raise NotImplementedError

    def version(self, obj, rev=None):
        raise NotImplementedError

    def object_version(self, obj, rev=None):
        raise NotImplementedError


class DatabaseRepository(IRepository):

    def __init__(self, model):
        self._model = model
        self._comparator = Comparator()

    def commit(self, obj, **info):
        latest_rev = self.version()
        prev_obj = self.object_version(obj, rev=latest_rev)
        if self._comparator.is_equal(prev_obj, obj):
            return

        delta = self._comparator.create_delta(prev_obj, obj)
        hash_ = hashlib.sha1(
            delta.encode("utf-8")
        ).hexdigest()
        rev = self._model(
            content_object=obj,
            revision=latest_rev.revision,
            hash=hash_,
            delta=delta,
            created_at=datetime.now(),
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

    def object_version(self, obj, rev=None):
        object_version = copy.copy(obj)
        for field_name in Registry()[obj.__class__]:
            setattr(object_version, field_name, None)
        revisions = self.revisions()
        if rev is not None:
            revisions = revisions.where((mapper_registry[self._model].sql_table.revision <= rev))
        for revision in revisions:
            self._comparator.apply_delta(object_version, revision.delta)
        return object_version

    def versions(self, obj):
        t = mapper_registry[self._model].sql_table
        return mapper_registry[self._model].query.where(
            (t.content_object == obj)
        ).order_by(
            t.revision
        )

    def version(self, obj, rev=None):
        t = mapper_registry[self._model].sql_table
        q = self.versions()
        if rev is not None:
            q = q.where((t.revision == rev))
        return q[0]


class Comparator(object):

    @staticmethod
    def encode_v2(val):
        # We are sending data to db in string format. Just use converters of connection here.
        if not isinstance(val, string_types):
            # XML? JSON? pickle?
            return ':'.join(PICKLED_MARKER, base64.standard_b64encode(
                pickle.dumps(val, protocol=pickle.HIGHEST_PROTOCOL)
            ).decode('ascii'))
        return ':'.join(STR_MARKER, val)  # prevent to user to falsify PICKLED_MARKER

    @staticmethod
    def decode_v2(val):
        marker, data = val.split(':', 1)
        if marker == PICKLED_MARKER:
            try:
                return pickle.loads(base64.standard_b64decode(data.encode('ascii')))
            except Exception:
                pass
        elif marker == STR_MARKER:
            return val

    @staticmethod
    def encode(val):
        if not isinstance(val, string_types):  # FIXME: Pickle all types?
            return ':'.join(PICKLED_MARKER, pickle.dumps(val, protocol=pickle.HIGHEST_PROTOCOL))
        return ':'.join(STR_MARKER, val)  # prevent to user to falsify PICKLED_MARKER

    @staticmethod
    def decode(val):
        marker, data = val.split(':', 1)
        if marker == PICKLED_MARKER:
            try:
                return pickle.loads(data.encode('ascii'))
            except Exception:
                pass
        elif marker == STR_MARKER:
            return val

    @staticmethod
    def _diff(prev_str, next_str):
        """Create a 'diff' from txt_prev to txt_new."""
        patch = dmp.patch_make(prev_str, next_str)
        return dmp.patch_toText(patch)

    def create_delta(self, prev_obj, next_obj):
        """Create a 'diff' from prev_obj to obj_new."""
        model = next_obj.__class__
        fields = Registry()[model]
        lines = []
        for field in fields:
            prev_value = getattr(prev_obj, field)
            next_value = getattr(next_obj, field)
            lines.extend(["--- {0}.{1}".format(model.__name__, field),
                          "+++ {0}.{1}".format(model.__name__, field)])
            if isinstance(next_value, string_types):
                if not isinstance(prev_value, string_types):
                    prev_value = ""
                # data_diff = unified_diff(data_prev.splitlines(), data_next.splitlines(), context=3)
                data_diff = self._diff(prev_value, next_value)
                lines.append(self.encode(data_diff.strip()))
            else:
                lines.append(self.encode(next_value))

        return "\n".join(lines)

    def apply_delta(self, obj, delta):
        model = obj.__class__
        fields = Registry()[model]
        diffs = self._split_delta_by_fields(delta)
        for key, diff_or_value in diffs.items():
            model_name, field_name = key.split('.')
            if model_name != model.__name__ or field_name not in fields:
                continue
            last_value = getattr(obj, field_name)
            if isinstance(last_value, string_types) and isinstance(diff_or_value, string_types):
                patch = dmp.patch_fromText(diff_or_value)
                prev_value = dmp.patch_apply(patch, last_value)[0]
            else:
                prev_value = diff_or_value
            setattr(obj, field_name, prev_value)

    @staticmethod
    def _split_delta_by_fields(txt):
        """Returns dictionary object, key is fieldname, value is it's diff"""
        result = {}
        current = None
        lines = txt.split("\n")
        # FIXME: prevent injection of field markers
        for line in lines:
            if line[:4] == "--- ":
                continue
            if line[:4] == "+++ ":
                line = line[4:].strip()
                result[line] = current = []
                continue
            if current is not None:
                current.append(line)
        for k, v in result.items():
            result[k] = "\n".join(v)
        return result

    def is_equal(self, prev_obj, next_obj):
        """Returns True, if watched attributes of obj1 deffer from obj2."""
        for field_name in Registry()[next_obj.__class__].fields:
            if getattr(prev_obj, field_name) != getattr(next_obj, field_name):
                return True
        return False

    def display_diff(self, prev_obj, next_obj):
        """Returns a HTML representation of the diff."""
        result = []
        for field_name in Registry()[next_obj.__class__].fields:
            result.append("<b>{0}</b>".format(field_name))
            prev_value = getattr(prev_obj, field_name)
            next_value = getattr(next_obj, field_name)
            if isinstance(prev_value, string_types) and isinstance(next_value, string_types):
                diffs = dmp.diff_main(prev_value, next_value)
                dmp.diff_cleanupSemantic(diffs)
                result.append(dmp.diff_prettyHtml(diffs))
            else:
                if prev_value != next_value:
                    result.append(
                        """
                        <span>
                            <del style="background:#ffe6e6;">{}</del>
                            <ins style="background:#e6ffe6;">{}</ins>
                        <span>
                        """.format(prev_value, next_value))
                else:
                    result.append("""<span>{}</span>""".format(next_value))
        return "<br />\n".join(result)


def unified_diff(fromlines, tolines, context=None):
    """
    Generator for unified diffs. Slightly modified version from Trac 0.11.
    """
    matcher = SequenceMatcher(None, fromlines, tolines)
    for group in matcher.get_grouped_opcodes(context):
        i1, i2, j1, j2 = group[0][1], group[-1][2], group[0][3], group[-1][4]
        if i1 == 0 and i2 == 0:
            i1, i2 = -1, -1  # add support
        yield '@@ -{0:d},{1:d} +{2:d},{3:d} @@'.format(i1 + 1, i2 - i1, j1 + 1, j2 - j1)
        for tag, i1, i2, j1, j2 in group:
            if tag == 'equal':
                for line in fromlines[i1:i2]:
                    yield ' ' + line
            else:
                if tag in ('replace', 'delete'):
                    for line in fromlines[i1:i2]:
                        yield '-' + line
                if tag in ('replace', 'insert'):
                    for line in tolines[j1:j2]:
                        yield '+' + line
