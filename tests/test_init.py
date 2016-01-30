import tubing
import sys

if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

class InitTestCase(unittest.TestCase):
    def test_version(self):
        self.assertRegexpMatches(tubing.version, "^0.0.1b.*")
        self.assertRegexpMatches(tubing.__version__, "0.0.1b.*")
