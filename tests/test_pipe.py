import logging
import os.path
import unittest2 as unittest
try:
    from StringIO import StringIO
except:
    from io import StringIO
from tubing import sinks, sources, pipe

SOURCE_DATA = [
    dict(name="Bob", age=38),
    dict(name="Carrie", age=38),
    dict(name="Devyn", age=18),
    dict(name="Calvin", age=13),
]
SERIALIZED_DATA = """{"age":38,"name":"Bob"}
{"age":38,"name":"Carrie"}
{"age":18,"name":"Devyn"}
{"age":13,"name":"Calvin"}
"""

expected0_path = os.path.join(os.path.dirname(__file__), 'expected0.gz')


logger = logging.getLogger("tubing.test_pipe")


class PipeTestCase(unittest.TestCase):
    def testPipe(self):
        buffer0 = StringIO()
        sink = sinks.JSONSerializerSink(buffer0, "\n", separators=(',', ':'))
        for obj in SOURCE_DATA:
            sink.write([obj])

        self.assertEqual(buffer0.getvalue(), SERIALIZED_DATA)

        buffer0.seek(0)

        buffer1 = sinks.StringIOSink()
        source = sources.JSONParserSource(sources.LineReaderSource(buffer0))
        sink = sinks.JSONSerializerSink(sinks.ZlibSink(sinks.BufferedSink(buffer1)))
        pipe.pipe(source, sink, amt=1)

        with file(expected0_path) as f:
            self.assertEqual(f.read(), buffer1.getvalue())
