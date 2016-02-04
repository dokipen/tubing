import logging
import tempfile
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


class SinksTestCase(unittest.TestCase):
    def testPipe(self):
        buffer0 = sinks.BytesIOSink()
        sink = sinks.JSONSerializerSink(
            sinks.BufferedSink(buffer0, batch_size=1), "\n", separators=(',', ':')
        )
        for obj in SOURCE_DATA:
            sink.write([obj])
        sink.done()

        buffer0.seek(0)

        # make chunk_size small to excersize chunking
        source = sources.LineReaderSource(buffer0, chunksize=10)
        self.assertNotEqual(source.read(1), [])
        self.assertNotEqual(source.read(1), [])
        self.assertNotEqual(source.read(1), [])
        self.assertNotEqual(source.read(1), [])
        self.assertEqual(source.read(1), [])

    def testBaseAndProxySink(self):
        base = sinks.BaseSink()
        self.assertIsNone(base.write('test'))
        self.assertIsNone(base.done())
        self.assertIsNone(base.abort())

        class MyProxy(sinks.ProxySink):
            def __init__(self, sink):
                self.sink = sink

        myproxy = MyProxy(sinks.BytesIOSink())
        myproxy.write(b'test')
        myproxy.done()
        myproxy.abort()
        self.assertEqual(b'test', myproxy.sink.getvalue())

    def testFileSink(self):
        tmp, path = tempfile.mkstemp()
        sink = sinks.FileSink(path)
        sink.write(b'hello')
        sink.done()
        sink = sinks.FileSink(path)
        sink.write(b'hello')
        sink.abort()
        os.unlink(path)

