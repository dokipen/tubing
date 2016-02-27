from __future__ import print_function
"""
Tubing sinks are targets for streams of data. To make your own sink,
define a Writer with a write(chunk) and optional close() and abort()
functions and pass it to MakeSink. if present, close() will be
called on the upstream EOF and abort() will be called on any
exception in the pipeline. Here's an example::

    class MyWriter(object):
        def write(self, chunk):
            print chunk

    MySink = MakeSink(MyWriter)
"""
import requests
import logging
import io
import functools
import hashlib

logger = logging.getLogger('tubing.sinks')


class SinkRunner(object):

    def __init__(self, apparatus, sink):
        self.apparatus = apparatus
        self.source = self.apparatus.tail()
        self.apparatus.sink = self
        self.sink = sink

    def __call__(self):
        try:
            logger.debug("reading %s", self.source)
            chunk, eof = self.source.read()
            self.sink.write(chunk)
            while not eof:
                chunk, eof = self.source.read()
                self.sink.write(chunk)
            hasattr(self.sink, 'close') and self.sink.close()
            return self.sink.writer
        except:
            logger.exception("Pipe failed")
            hasattr(self.sink, 'abort') and self.sink.abort()
            raise



class Sink(object):

    def __init__(self, writer):
        self.writer = writer

    def write(self, chunk):
        self.writer.write(chunk)
        return self

    def close(self):
        hasattr(self.writer, 'close') and self.writer.close()

    def abort(self):
        hasattr(self.writer, 'abort') and self.writer.abort()

    def result(self):
        if hasattr(self.writer, 'result'):
            return self.writer.result()
        return self.writer

    def receive(self, apparatus):
        SinkRunner(apparatus, self)()
        apparatus.result = self.result()
        return apparatus


def SinkFactory(writer_cls, *args, **kwargs):
    writer = writer_cls(*args, **kwargs)
    return Sink(writer)


def MakeSinkFactory(sink_cls):
    return functools.partial(SinkFactory, sink_cls)


class ObjectsSink(list):
    """
    Writes an object stream to a list.
    """

    def write(self, objs):
        self.extend(objs)


Objects = MakeSinkFactory(ObjectsSink)


class BytesWriter(io.BytesIO):
    """
    BytesWriter collects a stream of bytes and saves it to the result member
    on close().
    """

    def __init__(self, *args, **kwargs):
        super(BytesWriter, self).__init__(*args, **kwargs)

    def close(self):
        super(BytesWriter, self).close()

    def abort(self):
        super(BytesWriter, self).close()

    def result(self):
        return self.getvalue()


Bytes = MakeSinkFactory(BytesWriter)


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
    def __init__(self, url, username=None, password=None,
                 chunks_per_post=2**10, response_handler=lambda _: None):
        self.url = url
        if username:
            self.auth = (username, password)
        else:
            self.auth = None
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
            r, self.eof = self.source.read()
            if isinstance(r, list):
                for l in r:
                    yield l
            else:
                yield r
            if self.eof:
                return

    def receive(self, apparatus):
        self.source = apparatus.tail()
        apparatus.sink = self
        apparatus.result = []
        while not self.eof:
            r = requests.post(self.url, data=self.gen(), auth=self.auth)
            self.response_handler(r)
            apparatus.result.append(r)
        return apparatus


class DebugPrinter(object):

    def write(self, chunk):
        logger.error(chunk)

    def close(self):
        logger.error("CLOSED")

    def abort(self):
        logger.error("ABORTED")


Debugger = MakeSinkFactory(DebugPrinter)


class HashPrinter(object):
    def __init__(self, algorithm):
        self.hsh = hashlib.new(algorithm)

    def write(self, chunk):
        self.hsh.update(chunk)

    def result(self):
        return self.hsh


Hash = MakeSinkFactory(HashPrinter)

class CounterPrinter(object):
    def __init__(self):
        self.counter = 0

    def write(self, chunk):
        self.counter += len(chunk)

    def result(self):
        return self.counter

Counter = MakeSinkFactory(CounterPrinter)
