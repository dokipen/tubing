from __future__ import print_function
"""
This is where sources are connected to sinks.
"""
import logging
import json
import zlib
import gzip


logger = logging.getLogger('tubing.pipes')


DEBUG = False


# Some complicated closures to support our kickass API. This is the infamous
# factory-factory pattern in Python!
class MakePipe(object):
    def __init__(self, transformer_cls, default_chunk_size=2 ** 16):
        self.transformer_cls = transformer_cls
        self.default_chunk_size = default_chunk_size

    def __call__(self, chunk_size=None, *args, **kwargs):
        chunk_size = chunk_size or self.default_chunk_size
        def fn(source):
            transformer = self.transformer_cls(*args, **kwargs)
            return Pipe(source, chunk_size, transformer)
        return fn


class Pipe(object):
    """
    PipeMixin does all the grunt work that most pipe classes need to do.
    Children should implement _chunk and _close and make sure they have
    buffer, source, chunk_size and eof members. buffer is any iterator and eof
    is boolean.  source should have a read(int) method that returns an iterator
    that is the same type as buffer. chunk_size is an int and used to call
    source.read.
    """
    def __init__(self, source, chunk_size, transformer):
        self.source = source
        self.chunk_size = chunk_size
        self.transformer = transformer
        self.eof = False
        self.buffer = None

    def __or__(self, other):
        return other(self)

    def _read_complete(self, amt):
        return self.eof or amt and self.buffer and len(self.buffer) >= amt

    def _shift_buffer(self, amt):
        if self.buffer:
            r, self.buffer = self.buffer[:amt], self.buffer[amt or len(self.buffer):]
            return r
        else:
            return b''

    def _close(self):
        pass

    def append(self, chunk):
        if self.buffer:
            self.buffer += chunk
        else:
            self.buffer = chunk

    def read(self, amt=None):
        try:
            logger.debug("in read")
            while not self._read_complete(amt):
                inchunk, self.eof = self.source.read(self.chunk_size)
                if inchunk:
                    outchunk = self.transformer.transform(inchunk)
                    if outchunk:
                        self.append(outchunk)
                if self.eof and hasattr(self.transformer, 'close'):
                    self.append(self.transformer.close())

            if self.eof:
                if amt and len(self.buffer) > amt:
                    return self._shift_buffer(amt), False
                else:
                    return self._shift_buffer(amt), True
        except:
            logger.exception("Pipe failed")
            hasattr(self.transformer, 'abort') and self.transformer.abort()
            raise



class GunzipTransformer(object):
    """
    ZlibSource unzips a gzipped source stream.
    """
    def __init__(self):
        self.dec = zlib.decompressobj(32 + zlib.MAX_WBITS)

    def transform(self, chunk):
        return self.dec.decompress(chunk)


Gunzip = MakePipe(GunzipTransformer)


class GzipTransformer(object):
    """
    ZlibSink Gzips the binary input.
    """
    def __init__(self, compression=9):
        self.buffer = b''
        self.zipfile = gzip.GzipFile("", 'wb', compression, self)

    def write(self, b):
        self.buffer += b

    def transform(self, chunk):
        self.zipfile.write(chunk)
        r = self.buffer
        self.buffer = r''
        return r

    def close(self):
        self.zipfile.close()
        return self.buffer


Gzip = MakePipe(GzipTransformer)


class SplitTransformer(object):
    """
    SplitterPipe splits source data on a delimiter.
    """
    def __init__(self, on=b'\n'):
        self.on = on
        self.buffer = b'' # read buffer

    def transform(self, chunk):
        """
        We go through all this hoopla because returning nothing signals EOF.
        We keep reading chunks until real EOF or we get at least one part.
        """
        r = []
        self.buffer += chunk
        while self.on in self.buffer:
            out, self.buffer = self.buffer.split(self.on, 1)
            r.append(out)
        return r

    def close(self):
        return [self.buffer]


Split = MakePipe(SplitTransformer)


class JoinedTransformer(object):
    """
    JoinedTransformer does single level flattening of streams.
    """
    def __init__(self, by=b""):
        self.by = by
        self.first = True

    def transform(self, chunk):
        if self.first:
            return self.by.join(chunk)
        else:
            return self.by + self.by.join(chunk)


Joined = MakePipe(JoinedTransformer)


class JSONParserTransform(object):
    """
    JSONParserSource is not very smart. It expects a stream of complete raw
    JSON byte strings and works well with Delimiter for source files with one
    JSON object per line.
    """
    def __init__(self, encoding='utf-8'):
        self.encoding = encoding

    def transform(self, chunk):
        raws = filter(None, chunk)
        return [json.loads(raw.decode(self.encoding)) for raw in raws]


JSONParser = MakePipe(JSONParserTransform)


class JSONSerializerTransform(object):
    """
    JSONSerializer takes an object stream and serializes it to json byte strings.
    """

    def __init__(self, delimiter=u"\n", encoding="utf-8", **kwargs):
        self.delimiter = delimiter
        self.encoding = encoding
        self.json_kwargs = kwargs

    def transform(self, chunk):
        r = []
        for obj in chunk:
            line = json.dumps(obj, **self.json_kwargs)
            raw = line.encode(self.encoding)
            r.append(raw)
        return r


JSONSerializer = MakePipe(JSONSerializerTransform)
