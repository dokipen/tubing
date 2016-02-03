from __future__ import print_function
"""
Tubing sinks are targets for streams of data.
"""

import json
import gzip
import logging
try:
    from StringIO import StringIO
except:
    from io import StringIO


logger = logging.getLogger('tubing.sinks')


class BaseSink(object):
    """
    BaseSink is a mixin class for all Sinks providing empty impls of the sink
    interface.
    """

    def write(self, chunk):
        pass

    def done(self):
        pass

    def abort(self):
        pass


class ProxySink(object):
    """
    ProxySink is a mixin class with default implementations of all methods for a
    proxy sink.
    """

    def write(self, chunk):
        return self.sink.write(chunk)

    def done(self):
        return self.sink.done()

    def abort(self):
        return self.sink.abort()


class JSONSerializerSink(ProxySink):
    """
    JSONSerializerSink takes an object stream and serializes it to json.
    delimiter will be inserted after ever record.
    """

    def __init__(self, sink, delimiter="", **kwargs):
        self.sink = sink
        self.delimiter = delimiter
        self.json_kwargs = kwargs

    def write(self, objs):
        for obj in objs:
            line = json.dumps(obj, **self.json_kwargs)
            self.sink.write(line + self.delimiter)


class ZlibSink(ProxySink):
    """
    ZlibSink Gzips the binary input.
    """
    class InnerStream(StringIO):
        def __init__(self, sink):
            self.sink = sink

        def write(self, data):
            self.sink.write(data)

    def __init__(self, sink):
        self.sink = sink
        self.zipfile = gzip.GzipFile("", 'wb', 9, self.InnerStream(sink))

    def write(self, chunk):
        return self.zipfile.write(chunk)

    def done(self):
        self.zipfile.close()
        return super(ZlibSink, self).done()


class BufferedSink(ProxySink):
    """
    BufferedSink buffers output into batches before sending. It expects a text
    stream.
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
        return super(BufferedSink, self).done()


class FileSink(BaseSink):
    """
    FileSink writes data to a file.
    """
    def __init__(self, path):
        self.f = file(path, 'wb')

    def write(self, chunk):
        return self.f.write(chunk)

    def done(self):
        return self.f.close()

    def abort(self):
        return self.f.close()

class StringIOSink(StringIO, BaseSink):
    """
    StringIOSink wraps a StringIO object for tubing.
    """
    def done(self):
        pass

    def abort(self):
        pass

