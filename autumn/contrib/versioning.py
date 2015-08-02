from __future__ import absolute_import
import sys
import base64
import json

# Under construction!!! Not testet yet!!!

try:
    import cPickle as pickle
except ImportError:
    import pickle

# Use json instead pickle?

from difflib import SequenceMatcher

if sys.version_info > (3, ):
    from .vendor.diff_match_patch.python3.diff_match_patch import diff_match_patch
else:
    from .vendor.diff_match_patch.python2.diff_match_patch import diff_match_patch

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


class VersioningRegistry(dict):

    def __init__(self):
        if hasattr(VersioningRegistry, 'registry'):
            raise Exception("Already registered {}".format(
                type(VersioningRegistry.registry).__name__)
            )
        TranslationRegistry.registry = self

    def __call__(self, model, fields):
        if model._meta.name in self:
            raise AlreadyRegistered("Already registered {}".format(
                model.__name__)
            )

        self[model._meta.name] = d = {}


class Transaction(object):

    def __init__(self,):
        """Constructor of Transaction instance."""
        self.ctx = local()

    @property
    def scopes(self):
        """Get transaction scopes."""
        if not hasattr(self.ctx, 'transaction_scopes'):
            self.ctx.transaction_scopes = []
        return self.ctx.transaction_scopes

    def begin(self):
        """Begin transaction."""
        if self.locked(+1):
            return
        self.scopes.append(set())
        return self

    def commit(self):
        """Commit transaction"""
        if self.locked(-1):
            return
        scope = self.scopes.pop()
        for obj in scope:
            self.post_save(obj)

    def rollback(self):
        """Commit transaction"""
        if self.locked(-1):
            return
        self.scopes.pop()

    def lock(self):
        self.ctx.locked = 0
        return self

    def locked(self, val=None):
        if not hasattr(self.ctx, 'locked'):
            return False
        if val is not None:
            self.ctx.locked += val
        if self.ctx.locked == 0:
            delattr(self.ctx, 'locked')
            return False
        return True

    def add_obj(self, obj):
        """Adds object"""
        self.pre_save(obj)
        self.scopes[-1].add(obj)

    def pre_save(self, obj):
        """Pre-save object"""
        model = obj.__class__
        if not hasattr(obj, 'revision_info'):
            obj.revision_info = {}
        info = obj.revision_info

        try:
            prev = model._default_manager.get(pk=obj.pk)
        except model.DoesNotExist:
            prev = model()

        if not obj_is_changed(prev, obj):
            obj.revision_info = {}
            return

        info['delta'] = create_diff(prev, obj)
        request = getattr(self.ctx, 'request', None)
        if request:
            if not info.get('editor'):
                info['editor'] = request.user
            if not info.get('editor_ip'):
                info['editor_ip'] = request.META.get("REMOTE_ADDR")
        if not getattr(info.get('editor'), 'pk', None):  # Anonymuous
            info['editor'] = None

    def post_save(self, obj):
        """Post-save object"""
        info = getattr(obj, 'revision_info', {})
        if info:
            rev = Revision(**info)
            rev.content_object = obj
            rev.save()

    def __call__(self, f=None):
        if f is None:
            return self

        @wraps(f)
        def _decorated(*args, **kw):
            with self:
                rv = f(*args, **kw)
            return rv

        return _decorated

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            try:
                self.commit()
            except:
                self.rollback()
                raise

transaction = Transaction()


def pre_save(sender, instance, **kwargs):
    """Pre-save signal handler"""
    transaction.begin()
    transaction.add_obj(instance)


def post_save(sender, instance, **kwargs):
    """Post-save signal handler"""
    transaction.commit()


def revisions_for_object(instance):
    from .models import Revision
    return Revision.objects.get_for_object(instance)


def get_field_str(obj, field):
    """Returns field's string"""
    return obj._meta.get_field(field).value_to_string(obj)


class Comparator(object):

    _registry = {}

    @staticmethod
    def encode(val):
        if not isinstance(val, string_types):
            # XML? JSON? pickle?
            return ':'.join(PICKLED_MARKER, base64.standard_b64encode(
                pickle.dumps(val, protocol=pickle.HIGHEST_PROTOCOL)
            ).decode('ascii'))
        return ':'.join(STR_MARKER, val)  # prevent to user to falsify PICKLED_MARKER

    @staticmethod
    def decode(val):
        marker, data = val.split(':', 1)
        if marker == PICKLED_MARKER:
            try:
                return pickle.loads(base64.standard_b64decode(val[len(PICKLED_MARKER):].encode('ascii')))
            except Exception:
                pass
        elif marker == STR_MARKER:
            return val

    @staticmethod
    def _diff(prev_str, next_str):
        """Create a 'diff' from txt_prev to txt_new."""
        patch = dmp.patch_make(next_str, prev_str)
        return dmp.patch_toText(patch)

    def create_diff(self, prev_obj, next_obj):
        """Create a 'diff' from obj_prev to obj_new."""
        model = next_obj.__class__
        fields = self._registry[model]
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


def apply_diff(obj, delta):
    model = obj.__class__
    fields = _registry[model]
    diffs = diff_split_by_fields(delta)
    for key, diff in diffs.items():
        model_name, field_name = key.split('.')
        if model_name != model.__name__ or field_name not in fields:
            continue
        content = get_field_data(obj, field_name)
        patch = dmp.patch_fromText(diff)
        content = dmp.patch_apply(patch, content)[0]
        set_field_data(obj, field_name, content)


def obj_is_changed(obj_prev, obj_next):
    """Returns True, if watched attributes of obj1 deffer from obj2."""
    model = obj_next.__class__
    fields = _registry[model]
    for field in fields:
        original_data = get_field_data(obj_next, field)
        new_data = get_field_data(obj_prev, field)
        if original_data != new_data:
            return True
    return False


def display_diff(obj_prev, obj_next):
    """Returns a HTML representation of the diff."""
    model = obj_next.__class__
    fields = _registry[model]

    result = []
    for field_name in fields:
        result.append("<b>{0}</b>".format(field_name))
        diffs = dmp.diff_main(
            get_field_str(obj_prev, field_name),
            get_field_str(obj_next, field_name)
        )
        dmp.diff_cleanupSemantic(diffs)
        result.append(dmp.diff_prettyHtml(diffs))
    return "<br />\n".join(result)


def diff_split_by_fields(txt):
    """Returns dictionary object, key is fieldname, value is it's diff"""
    result = {}
    current = None
    lines = txt.split("\n")
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
