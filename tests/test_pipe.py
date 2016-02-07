import logging
import os.path
import unittest2 as unittest
from io import BytesIO
from tubing import sinks, sources, pipes

SOURCE_DATA = [
    dict(name="Bob", age=38),
    dict(name="Carrie", age=38),
    dict(name="Devyn", age=18),
    dict(name="Calvin", age=13),
]

expected0_path = os.path.join(os.path.dirname(__file__), 'expected0.gz')


logger = logging.getLogger("tubing.test_pipe")




class PipeTestCase(unittest.TestCase):

    def testPipe(self):
        source = sources.Objects(SOURCE_DATA)
        sink = source | pipes.JSONSerializer(separators=(',', ':')) \
                      | pipes.Joined(by=b"\n") \
                      | pipes.Gzip() \
                      | pipes.Gunzip() \
                      | pipes.Split(on=b"\n") \
                      | pipes.JSONParser() \
                      | sinks.Objects()

        self.assertEqual(sink[0], SOURCE_DATA[0])
        self.assertEqual(sink[1], SOURCE_DATA[1])
        self.assertEqual(sink[2], SOURCE_DATA[2])
        self.assertEqual(sink[3], SOURCE_DATA[3])


    def testFailingSink(self):
        _abort = False
        _done = False

        class FailSink(object):
            def write(self, _):
                raise ValueError("Meant to fail")

            def done(self):
                _done = True

            def abort(self):
                _abort = True

        Fail = sinks.gen_fn(FailSink)

        try:
            sources.Objects(SOURCE_DATA) | Fail()
            self.assert_(False, "Expected Failure")
        except ValueError:
            pass

        #self.assert_(sink._abort)
        self.assert_(not _done)
