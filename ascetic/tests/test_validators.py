import unittest

from ascetic import exceptions, validators


class TestValidators(unittest.TestCase):

    maxDiff = None

    def test_validators(self):
        ev = validators.Email()
        assert ev('test@example.com')
        assert not ev('adsf@.asdf.asdf')
        assert validators.Length()('a')
        assert not validators.Length(2)('a')
        assert validators.Length(max_length=10)('abcdegf')
        assert not validators.Length(max_length=3)('abcdegf')

        n = validators.Number(1, 5)
        assert n(2)
        assert not n(6)
        assert validators.Number(1)(100.0)
        assert not validators.Number()('rawr!')

        vc = validators.ChainValidator(validators.Length(8), validators.Email())
        self.assertTrue(vc('test@example.com'))
        with self.assertRaises(exceptions.ValidationError):
            vc('a@a.com')
        with self.assertRaises(exceptions.ValidationError):
            vc('asdfasdfasdfasdfasdf')
