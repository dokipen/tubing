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
        sink = sinks.Objects()
        source |  pipes.JSONSerializer(separators=(',', ':')) | pipes.Joined(by=b"\n") | pipes.Gzip() | pipes.Gunzip() | pipes.Split(on=b"\n") | pipes.JSONParser() | sink

        self.assertEqual(sink[0], SOURCE_DATA[0])
        self.assertEqual(sink[1], SOURCE_DATA[1])
        self.assertEqual(sink[2], SOURCE_DATA[2])
        self.assertEqual(sink[3], SOURCE_DATA[3])


    def xtestFailingSink(self):
        class FailSink(object):
            def __init__(self):
                self._done = False
                self._abort = False

            def write(self, *args, **kwargs):
                raise ValueError("Meant to fail")

            def done(self):
                self._done = True

            def abort(self):
                self._abort = True

        buffer0 = sinks.BytesIOSink()
        sink = sinks.JSONSerializerSink(buffer0, "\n", separators=(',', ':'))
        for obj in SOURCE_DATA:
            sink.write([obj])
        sink.done()

        buffer0.seek(0)
        s = FailSink()
        try:
            pipe.pipe(buffer0, s)
            self.assert_(False, "Expected Failure")
        except ValueError:
            pass

        self.assert_(s._abort)
        self.assert_(not s._done)
