from __future__ import print_function
"""
Tubing sinks are targets for streams of data..
"""

import json
import StringIO
import gzip
import logging


logger = logging.getLogger('tubing.sinks')


class BaseSink(object):
    def write(self, chunk):
        pass

    def done(self):
        pass

    def abort(self):
        pass


class ProxySink(object):
    """
    expects self.sink
    """

    def write(self, chunk):
        return self.sink.write(chunk)

    def done(self):
        return self.sink.done()

    def abort(self):
        return self.sink.abort()


class JSONSerializerSink(ProxySink):
    def __init__(self, sink):
        self.sink = sink

    def write(self, obj):
        return self.sink.write(json.dumps(ob))


class ZlibSink(StringIO.StringIO, ProxySink):
    class InnerStream(StringIO.StringIO):
        def __init__(self, sink):
            self.sink = sink

        def write(self, data):
            self.sink.write(data)

    def __init__(self, sink):
        self.sink = sink
        self.zipfile = gzip.GzipFile("", 'wb', 9, InnerStream(sink))

    def write(self, chunk):
        return self.zipfile.write(chunk)

    def done(self):
        self.zipfile.close()
        return super(ProxySink, self).done()


class BufferedSink(ProxySink):
    """
    Buffer output into batches before sending.
    """
    def __init__(self, sink, batch_size=6000000):
        self.buffer = ""
        self.batch_size = batch_size
        self.sink = sink

    def write(self, chunk):
        """
        write media docs to sink.
        """
        self.buffer += chunk
        while len(self.buffer) >= self.batch_size:
            r = self.sink.write(self.buffer[:self.batch_size])
            self.buffer = self.buffer[self.batch_size:]
            return r

    def done(self):
        """
        finalize sink, flushing the buffer to downstream sink.
        """
        self.sink.write(self.buffer)
        return super(ProxySink, self).done()


class FileSink(SinkBase):
    def __init__(self, path):
        self.f = file(path, 'wb')

    def write(self, chunk):
        return self.f.write(chunk)

    def done(self):
        return self.f.close()

    def abort(self):
        return self.f.close()
