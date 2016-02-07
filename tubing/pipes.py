from __future__ import print_function
"""
This is where sources are connected to sinks.
"""
import logging
import json
import zlib
import gzip
import io


logger = logging.getLogger('tubing.pipes')


# Some complicated closures to support our kickass API. This is the infamous
# factory-factory pattern in Python!
def gen_fn(pipe_cls):
    def gen(*args, **kwargs):
        def fn(source):
            return pipe_cls(source, *args, **kwargs)
        return fn
    return gen


class PipeMixin(object):
    """
    PipeMixin does all the grunt work that most pipe classes need to do.
    Children should implement _chunk and _close and make sure they have
    buffer, source, chunk_size and eof members. buffer is any iterator and eof
    is boolean.  source should have a read(int) method that returns an iterator
    that is the same type as buffer. chunk_size is an int and used to call
    source.read.
    """
    def __or__(self, other):
        return other(self)

    def _read_complete(self, amt):
        return self.eof and amt and amt > len(self.buffer)

    def _shift_buffer(self, amt):
        r, self.buffer = self.buffer[:amt], self.buffer[amt or len(self.buffer):]
        return r

    def _chunk(self):
        return self.source.read(self.chunk_size)

    def _close(self):
        pass

    def read(self, amt=None):
        while not self._read_complete(amt):
            chunk = self._chunk()
            if chunk:
                self.buffer += chunk
            else:
                self.eof = True
                self._close()
                break

        return self._shift_buffer(amt)


class GunzipPipe(PipeMixin):
    """
    ZlibSource unzips a gzipped source stream.
    """
    def __init__(self, source, chunk_size=2 ** 16):
        self.source = source
        self.dec = zlib.decompressobj(32 + zlib.MAX_WBITS)
        self.chunk_size = chunk_size
        self.buffer = b''
        self.eof = False

    def _chunk(self):
        r = b''
        while not self.eof and len(r) == 0:
            chunk = self.source.read(self.chunk_size)
            if chunk:
                r += self.dec.decompress(chunk)
            else:
                self.eof = True
        return r


Gunzip = gen_fn(GunzipPipe)


class GzipPipe(PipeMixin):
    """
    ZlibSink Gzips the binary input.
    """
    def __init__(self, source, chunk_size=2 ** 16, compression=9):
        self.source = source
        self.chunk_size = chunk_size
        self.inbuffer = b''
        self.buffer = b''
        self.eof = False
        self.zipfile = gzip.GzipFile("", 'wb', 9, self)

    def write(self, b):
        self.inbuffer += b

    def _chunk(self):
        while not self.eof and len(self.inbuffer) == 0:
            chunk = self.source.read(self.chunk_size)
            if chunk:
                self.zipfile.write(chunk)
            else:
                self.zipfile.close()
                self.eof = True

        r = self.inbuffer
        self.inbuffer = r''
        return r


Gzip = gen_fn(GzipPipe)


class SplitPipe(PipeMixin):
    """
    SplitterPipe splits source data on a delimiter.
    """
    def __init__(self, source, chunk_size=2 ** 16, on=b'\n'):
        self.source = source
        self.chunk_size = chunk_size
        self.on = on
        self.chunk_buffer = b'' # read buffer
        self.buffer = [] # lines buffer
        self.eof = False

    def _chunk(self):
        """
        We go through all this hoopla because returning nothing signals EOF.
        We keep reading chunks until real EOF or we get at least one part.
        """
        r = []
        while not r and not self.eof:
            chunk = self.source.read(self.chunk_size)
            if chunk:
                self.chunk_buffer += chunk
                while self.on in self.chunk_buffer:
                    out, self.chunk_buffer = self.chunk_buffer.split(self.on, 1)
                    r.append(out)
            else:
                r.append(self.chunk_buffer)
                self.chunk_buffer = b''
                self.eof = True
        return r


Split = gen_fn(SplitPipe)


class JoinedPipe(PipeMixin):
    """
    JoinedPipe does single level flattening of streams.
    """
    def __init__(self, source, chunk_size=2 ** 16, by=b""):
        self.source = source
        self.chunk_size = chunk_size
        self.by = by
        self.buffer = b''
        self.eof = False

    def _chunk(self):
        return self.by.join(self.source.read(self.chunk_size))


Joined = gen_fn(JoinedPipe)


class JSONParserPipe(PipeMixin):
    """
    JSONParserSource is not very smart. It expects a stream of complete raw
    JSON byte strings and works well with Delimiter for source files with one
    JSON object per line.
    """
    def __init__(self, source, chunk_size=2 ** 8, encoding='utf-8'):
        self.source = source
        self.chunk_size = chunk_size
        self.encoding = encoding
        self.buffer = []
        self.eof = False

    def _chunk(self):
        raws = filter(None, self.source.read(self.chunk_size))
        return [json.loads(raw.decode(self.encoding)) for raw in raws]


JSONParser = gen_fn(JSONParserPipe)


class JSONSerializerPipe(PipeMixin):
    """
    JSONSerializer takes an object stream and serializes it to json byte strings.
    """

    def __init__(self, source, chunk_size=2 * 8, delimiter=u"\n", encoding="utf-8", **kwargs):
        self.source = source
        self.delimiter = delimiter
        self.json_kwargs = kwargs
        self.encoding = encoding
        self.chunk_size = chunk_size
        self.buffer = []
        self.eof = False

    def _chunk(self):
        objs = self.source.read(self.chunk_size)
        r = []
        for obj in objs:
            line = json.dumps(obj, **self.json_kwargs)
            raw = line.encode(self.encoding)
            r.append(raw)
        return r


JSONSerializer = gen_fn(JSONSerializerPipe)
