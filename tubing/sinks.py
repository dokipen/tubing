from __future__ import print_function
"""
Tubing sinks are targets for streams of data. To make your own sink, define a Writer with a write(chunk) and optional close() and abort() functions and pass it to MakeSink. if present, close() will be called on the upstream EOF and abort() will be called on any exception in the pipeline. Here's an example::

    class MyWriter(object):
        def write(self, chunk):
            print chunk

    MySink = MakeSink(MyWriter)
"""
import logging
import io

logger = logging.getLogger('tubing.sinks')


class MakeSink(object):
    """
    MakeSink returns a factory that wraps a Writer and executes the pipeline.
    default_chunk_size should be set according to the type of stream that the
    sink works on. Objects are heavier than byte streams, so should have a
    lower default_chunk_size.
    """

    def __init__(self, sink_cls, default_chunk_size=2**16):
        self.sink_cls = sink_cls
        self.default_chunk_size = default_chunk_size

    def __call__(self, chunk_size=None, *args, **kwargs):
        chunk_size = chunk_size or self.default_chunk_size

        def fn(source):
            try:
                sink = self.sink_cls(*args, **kwargs)
                chunk, eof = source.read(chunk_size)
                sink.write(chunk)
                while not eof:
                    chunk, eof = source.read(chunk_size)
                    sink.write(chunk)
                hasattr(sink, 'close') and sink.close()
                return sink
            except:
                logger.exception("Pipe failed")
                hasattr(sink, 'abort') and sink.abort()
                raise

        return fn


class ObjectsSink(list):
    """
    Writes an object stream to a list.
    """

    def write(self, objs):
        self.extend(objs)


Objects = MakeSink(ObjectsSink, 2**4)


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


Bytes = MakeSink(BytesWriter, 2**16)
