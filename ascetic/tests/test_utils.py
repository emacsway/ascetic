import unittest

from ascetic import utils


class TestUtils(unittest.TestCase):

    maxDiff = None

    def test_resolve(self):
        from ascetic.databases import Database
        self.assertTrue(utils.resolve('ascetic.databases.Database') is Database)
