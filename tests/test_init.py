import unittest
import tubing

class InitTestCase(unittest.TestCase):
    def test_version(self):
        self.assert_(tubing.version.startswith("0.0.1-a"))
        self.assert_(tubing.__version__.startswith("0.0.1-a"))
