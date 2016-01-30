import unittest
import tubing

class InitTestCase(unittest.TestCase):
    def test_version(self):
        self.assertRegexpMatches(tubing.version, "^0.0.1b.*")
        self.assertRegexpMatches(tubing.__version__, "0.0.1b.*")
