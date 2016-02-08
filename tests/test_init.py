import tubing
# For python 2.6 backports
import unittest2 as unittest


class InitTestCase(unittest.TestCase):

    def test_version(self):
        self.assertRegexpMatches(tubing.version, "^0\.0\.2.*")
        self.assertRegexpMatches(tubing.__version__, "0\.0\.2*")
