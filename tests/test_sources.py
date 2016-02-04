import logging
import os.path
import unittest2 as unittest
from io import BytesIO
from tubing import sources, sinks, pipe

SOURCE_DATA = [
    dict(name="Bob", age=38),
    dict(name="Carrie", age=38),
    dict(name="Devyn", age=18),
    dict(name="Calvin", age=13),
]

expected0_path = os.path.join(os.path.dirname(__file__), 'expected0.gz')


logger = logging.getLogger("tubing.test_pipe")


class SourcesTestCase(unittest.TestCase):
    def testJSONSerializerSink(self):
        buffer0 = BytesIO()
        sink = sinks.JSONSerializerSink(buffer0, "\n", separators=(',', ':'))
        for obj in SOURCE_DATA:
            sink.write([obj])

        buffer0.seek(0)

        # make chunk_size small to excersize chunking
        source = sources.LineReaderSource(buffer0, chunksize=10)
        self.assertNotEqual(source.read(1), [])
        self.assertNotEqual(source.read(1), [])
        self.assertNotEqual(source.read(1), [])
        self.assertNotEqual(source.read(1), [])
        self.assertEqual(source.read(1), [])
        self.assertEqual(source.read(1), [])

        buffer0.seek(0)

        # make chunk_size large to excersize splits
        source = sources.LineReaderSource(buffer0, chunksize=1000000)
        self.assertNotEqual(source.read(1), [])
        self.assertNotEqual(source.read(1), [])
        self.assertNotEqual(source.read(1), [])
        self.assertNotEqual(source.read(1), [])
        self.assertEqual(source.read(1), [])
        self.assertEqual(source.read(1), [])
