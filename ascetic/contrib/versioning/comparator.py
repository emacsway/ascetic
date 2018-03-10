from __future__ import absolute_import
import sys

from difflib import SequenceMatcher
from ascetic.contrib.versioning.interfaces import IComparator


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

dmp = diff_match_patch()


class Comparator(IComparator):

    def __init__(self, registry):
        """
        :type registry: ascetic.contrib.versioning.interfaces.IRegistry
        """
        self._registry = registry

    @staticmethod
    def _diff(prev_str, next_str):
        """Create a 'diff' from txt_prev to txt_new."""
        patch = dmp.patch_make(prev_str, next_str)
        return dmp.patch_toText(patch)

    def create_delta(self, prev_obj, next_obj):
        """Create a 'diff' from prev_obj to obj_new."""
        model = next_obj.__class__
        object_accessor = self._registry.get_object_accessor(model)
        fields = self._registry.get_fields(model)
        result = dict()
        for field in fields:
            prev_value = object_accessor.get_value(prev_obj, field)
            next_value = object_accessor.get_value(next_obj, field)
            if isinstance(next_value, string_types):
                if not isinstance(prev_value, string_types):
                    prev_value = ""
                # data_diff = unified_diff(data_prev.splitlines(), data_next.splitlines(), context=3)
                data_diff = self._diff(prev_value, next_value)
                result["{0}.{1}".format(model.__name__, field)] = data_diff.strip()
            else:
                result["{0}.{1}".format(model.__name__, field)] = next_value
        return result

    def apply_delta(self, obj, delta):
        model = obj.__class__
        object_accessor = self._registry.get_object_accessor(model)
        fields = self._registry.get_fields(model)
        for key, diff_or_value in delta.items():
            model_name, field_name = key.split('.')
            if model_name != model.__name__ or field_name not in fields:
                continue
            last_value = object_accessor.get_value(obj, field_name)
            if isinstance(last_value, string_types) and isinstance(diff_or_value, string_types):
                patch = dmp.patch_fromText(diff_or_value)
                prev_value = dmp.patch_apply(patch, last_value)[0]
            else:
                prev_value = diff_or_value
            setattr(obj, field_name, prev_value)

    def is_equal(self, prev_obj, next_obj):
        """Returns True, if watched attributes of obj1 deffer from obj2."""
        model = next_obj.__class__
        object_accessor = self._registry.get_object_accessor(model)
        for field_name in self._registry.get_fields(model):
            if object_accessor.get_value(prev_obj, field_name) != object_accessor.get_value(next_obj, field_name):
                return True
        return False

    def display_diff(self, prev_obj, next_obj):
        """Returns a HTML representation of the diff."""
        model = next_obj.__class__
        object_accessor = self._registry.get_object_accessor(model)
        result = []
        for field_name in self._registry.get_fields(model):
            result.append("<b>{0}</b>".format(field_name))
            prev_value = object_accessor.get_value(prev_obj, field_name)
            next_value = object_accessor.get_value(next_obj, field_name)
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
