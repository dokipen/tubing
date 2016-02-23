from __future__ import print_function
"""
Tubing sinks are targets for streams of data. To make your own sink, define a Writer with a write(chunk) and optional close() and abort() functions and pass it to MakeSink. if present, close() will be called on the upstream EOF and abort() will be called on any exception in the pipeline. Here's an example::

    class MyWriter(object):
        def write(self, chunk):
            print chunk

    MySink = MakeSink(MyWriter)
"""
import requests
import logging
import io
import functools

logger = logging.getLogger('tubing.sinks')


class SinkRunner(object):

    def __init__(self, source, sink, chunk_size):
        self.source = source
        self.sink = sink
        self.chunk_size = chunk_size

    def __call__(self):
        try:
            logger.debug("reading %s" % (self.source))
            chunk, eof = self.source.read(self.chunk_size)
            self.sink.write(chunk)
            while not eof:
                chunk, eof = self.source.read(self.chunk_size)
                self.sink.write(chunk)
            hasattr(self.sink, 'close') and self.sink.close()
            return self.sink.writer
        except:
            logger.exception("Pipe failed")
            hasattr(self.sink, 'abort') and self.sink.abort()
            raise


class Sink(object):

    def __init__(self, writer, chunk_size=2**16):
        self.writer = writer
        self.chunk_size = chunk_size

    def write(self, chunk):
        self.writer.write(chunk)
        return self

    def close(self):
        hasattr(self.writer, 'close') and self.writer.close()

    def abort(self):
        hasattr(self.writer, 'abort') and self.writer.abort()

    def receive(self, source):
        SinkRunner(source, self, self.chunk_size)()
        return self.writer


def SinkFactory(writer_cls, default_chunk_size=2**16, *args, **kwargs):
    if kwargs.get('chunk_size'):
        chunk_size = kwargs['chunk_size']
        del kwargs['chunk_size']
    else:
        chunk_size = default_chunk_size

    writer = writer_cls(*args, **kwargs)
    return Sink(writer, chunk_size)


def MakeSinkFactory(sink_cls, default_chunk_size=2**16):
    return functools.partial(SinkFactory, sink_cls, default_chunk_size)


class ObjectsSink(list):
    """
    Writes an object stream to a list.
    """

    def write(self, objs):
        self.extend(objs)


Objects = MakeSinkFactory(ObjectsSink, 2**4)


class BytesWriter(io.BytesIO):
    """
    BytesWriter collects a stream of bytes and saves it to the result member
    on close().
    """

    def __init__(self, *args, **kwargs):
        self.result = None
        super(BytesWriter, self).__init__(*args, **kwargs)

    def close(self):
        self.result = self.getvalue()
        super(BytesWriter, self).close()

    def abort(self):
        super(BytesWriter, self).close()


Bytes = MakeSinkFactory(BytesWriter, 2**16)


class FileWriter(object):

    def __init__(self, *args, **kwargs):
        self.f = open(*args, **kwargs)

    def write(self, chunk):
        self.f.write(chunk)

    def close(self):
        self.f.close()

    def abort(self):
        # TODO: probably delete the file
        self.f.close()


File = MakeSinkFactory(FileWriter)


class HTTPPost(object):
    """
    HTTPPost doesn't support the write method, and therefore can not be used
    with tubes.Tee.
    """
    def __init__(self, url, username=None, password=None, chunk_size=2**32,
                 chunks_per_post=2**10, response_handler=lambda _: None):
        self.url = url
        if username:
            self.auth = (username, password)
        else:
            self.auth = None
        self.chunk_size = chunk_size
        self.chunks_per_post = chunks_per_post
        self.response_handler = response_handler
        self.eof = False

    def gen(self):
        """
        We have to do this goofy shit because requests.post doesn't give
        access to the socket directly. In order to stream, we need to pass
        a generator object to requests.
        """
        for _ in range(self.chunks_per_post):
            r, self.eof = self.source.read(self.chunk_size)
            yield r
            if self.eof:
                return

    def receive(self, other):
        self.source = other
        while not self.eof:
            r = requests.post(self.url, data=self.gen(), auth=self.auth)
            self.response_handler(r)


class DebugPrinter(object):

    def write(self, chunk):
        logger.debug(chunk)

    def close(self):
        logger.debug("CLOSED")

    def abort(self):
        logger.debug("ABORTED")


Debugger = MakeSinkFactory(DebugPrinter)
