from __future__ import absolute_import
import base64
from ascetic.contrib.versioning.interfaces import ICodecAdapter, ISerializer

try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    str = unicode  # Python 2.* compatible
    string_types = (basestring,)
    integer_types = (int, long)
except NameError:
    string_types = (str,)
    integer_types = (int,)

PICKLED_MARKER = 'pickled'
STR_MARKER = 'str'


class Encoder2(ICodecAdapter):

    def encode(self, decoded):
        # We are sending data to db in string format. Just use converters of connection here.
        if not isinstance(decoded, string_types):
            # XML? JSON? pickle?
            return ':'.join((PICKLED_MARKER, base64.standard_b64encode(
                pickle.dumps(decoded, protocol=pickle.HIGHEST_PROTOCOL)
            ).decode('ascii')))
        return ':'.join((STR_MARKER, decoded))  # prevent to user to falsify PICKLED_MARKER

    def decode(self, encoded):
        marker, data = encoded.split(':', 1)
        if marker == PICKLED_MARKER:
            try:
                return pickle.loads(base64.standard_b64decode(data.encode('ascii')))
            except Exception:
                pass
        elif marker == STR_MARKER:
            return encoded


class Encoder(ICodecAdapter):

    def encode(self, decoded):
        if not isinstance(decoded, string_types):
            return ':'.join((PICKLED_MARKER, pickle.dumps(decoded, protocol=0)))
        return ':'.join((STR_MARKER, decoded))  # prevent to user to falsify PICKLED_MARKER

    def decode(self, encoded):
        marker, data = encoded.split(':', 1)
        if marker == PICKLED_MARKER:
            try:
                return pickle.loads(data.encode('ascii'))
            except Exception:
                pass
        elif marker == STR_MARKER:
            return encoded


class Serializer(ISerializer):
    VERSION = 1

    def __init__(self, encoder):
        """
        :type encoder: ascetic.contrib.versioning.interfaces.ICodecAdapter
        """
        self._encoder = encoder

    def is_acceptable(self, dump):
        version = int(dump.split('\n', 1).pop(0).split('version:', 1).pop(1))
        return version == self.VERSION

    def dumps(self, payload):
        lines = ['version:{0}'.format(self.VERSION)]
        for field, value in payload.items():
            lines.extend(["--- {0}".format(field),
                          "+++ {0}".format(field)])
            lines.append(self._encoder.encode(value))
        return "\n".join(lines)

    def loads(self, dump):
        result = {}
        current = None
        lines = dump.split("\n")
        # FIXME: prevent injection of field markers???
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
