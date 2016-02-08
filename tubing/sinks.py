from __future__ import print_function
"""
Tubing sinks are targets for streams of data.
"""
import logging
import io

logger = logging.getLogger('tubing.sinks')


class MakeSink(object):

    def __init__(self, sink_cls, default_chunk_size=None):
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

    def write(self, objs):
        self.extend(objs)


Objects = MakeSink(ObjectsSink, 2**4)


class BytesIOSink(io.BytesIO):

    def __init__(self, *args, **kwargs):
        self.result = None
        super(BytesIOSink, self).__init__(*args, **kwargs)

    def close(self):
        self.result = self.getvalue()
        super(BytesIOSink, self).close()

    def abort(self):
        super(BytesIOSink, self).close()


BytesIO = MakeSink(BytesIOSink)
