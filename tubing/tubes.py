from __future__ import print_function
"""
This is where all transformations in the tube occur. To make your own tube,
define a Transformer object and pass it to MakeTransformerTubeFactory.

If you don't need state, you can pass a transformer function directly to a
ChunkMap or Map Tube.
"""
import logging
try:
    import ujson as json
except:
    import json
import zlib
import gzip
import functools
import os

logger = logging.getLogger('tubing.tubes')

OBJ_CHUNK_SIZE = int(os.environ.get("OBJ_CHUNK_SIZE", 2**3))
BYTE_CHUNK_SIZE = int(os.environ.get("BYTE_CHUNK_SIZE", 2**18))


def TransformerTubeFactory(default_chunk_size=BYTE_CHUNK_SIZE):
    """
    TransformerTubeFactory is a decorator to turn a Transformer class into a
    TransformerTubeFactory.
    """

    def wrapper(cls):
        return MakeTransformerTubeFactory(cls, default_chunk_size)

    return wrapper


def MakeTransformerTubeFactory(transformer_cls,
                               default_chunk_size=BYTE_CHUNK_SIZE):
    """
    Returns a TransformerTubeFactory, which in turn, returns a TransformerTube.
    """
    return functools.partial(
        TransformerTube, transformer_cls, default_chunk_size
    )


class TransformerTube(object):
    """
    TransformerTube is what is returned by a TransformerTubeFactory. It
    manages the initialization of the TransformerTubeWorker.
    """

    def __init__(self, transformer_cls, default_chunk_size, *args, **kwargs):
        self.chunk_size = default_chunk_size
        if kwargs.get("chunk_size"):
            self.chunk_size = kwargs["chunk_size"]
            del kwargs["chunk_size"]

        self.transformer_cls = transformer_cls
        self.args = args
        self.kwargs = kwargs

    def receive(self, apparatus):
        transformer = self.transformer_cls(*self.args, **self.kwargs)
        return TransformerTubeWorker(apparatus, self.chunk_size, transformer)


class TransformerTubeWorker(object):
    """
    TransformerTubeWorker wraps a Transformer and does all the grunt work that
    most tubes need to do.  Transformers should implement transform(chunk), and
    optionally close() and abort().
    """

    def __init__(self, apparatus, chunk_size, transformer):
        self.apparatus = apparatus
        self.source = self.apparatus.tail()
        self.apparatus.tubes.append(self)
        if not chunk_size:
            raise ValueError("no chunk size")
        self.chunk_size = chunk_size
        self.transformer = transformer
        self.eof = False
        self.buffer = None
        self.result = None

    def __or__(self, other):
        return self.tube(other)

    def tube(self, other):
        return other.receive(self.apparatus)

    def read_complete(self):
        """
        read_complete tells us if the current request is fulfilled. It's fulfilled
        if we've reached the EOF in the source, or we have $amt parts. If amt is
        None, we should read to the source's EOF.
        """
        buff_len = len(self.buffer or [])
        logger.debug("[%s] buffer: %d of %d", self.transformer, buff_len,
                     self.chunk_size)
        return self.eof or (self.buffer and buff_len >= self.chunk_size)

    def shift_buffer(self, amt):
        """
        Remove $amt data from the front of the buffer and return it.
        """
        if self.buffer:
            r, self.buffer = self.buffer[:amt], self.buffer[
                amt or len(
                    self.buffer
                ):
            ]
            return r
        else:
            return b''

    def append(self, chunk):
        """
        append to the buffer, creating it if it doesn't exist.
        """
        if self.buffer and chunk:
            self.buffer += chunk
        else:
            self.buffer = chunk

    def buffer_len(self):
        """
        buffer_len even if buffer is None.
        """
        return self.buffer and len(self.buffer) or 0

    def read(self):
        """
        This is where the rubber meets the snow.
        """
        logger.debug("[%s] Reading %s", self.transformer, self.chunk_size)
        try:
            while not self.read_complete():
                inchunk, self.eof = self.source.read()
                if inchunk:
                    outchunk = self.transformer.transform(inchunk)
                    if outchunk:
                        self.append(outchunk)
                if self.eof and hasattr(self.transformer, 'close'):
                    c = self.transformer.close()
                    if c:
                        self.append(c)
                    if hasattr(self.transformer, 'result'):
                        self.result = self.transformer.result

            if self.eof and (self.buffer_len() <= self.chunk_size):
                # We've written everything, we're done
                return self.shift_buffer(self.chunk_size), True

            return self.shift_buffer(self.chunk_size), False
        except:
            logger.exception("Tube failed")
            hasattr(self.transformer, 'abort') and self.transformer.abort()
            raise

    def read_iterator(self):
        return TubeIterator(self)

    def gen(self):
        while True:
            r, eof = self.read()
            yield r
            if eof:
                return


class TubeIterator(object):
    """
    TubeIterator wraps a tube in an iterator object.
    """

    def __init__(self, tube):
        self.tube = tube
        self.eof = False

    def next(self):
        if self.eof:
            logger.debug("iter stopped")
            raise StopIteration

        r, self.eof = self.tube.read()
        logger.debug("iter >> %s", r)
        return r

    def __next__(self):
        """
        Python 3 support
        """
        return self.next()

    def __iter__(self):
        return self


@TransformerTubeFactory()
class Gunzip(object):
    """
    Gunzip unzips a gzipped source stream.
    """

    def __init__(self):
        self.dec = zlib.decompressobj(32 + zlib.MAX_WBITS)

    def transform(self, chunk):
        return self.dec.decompress(chunk) or b''


@TransformerTubeFactory()
class Gzip(object):
    """
    Gzip Gzips the binary input.
    """

    def __init__(self, compression=9):
        self.buffer = b''
        self.zipfile = gzip.GzipFile("", 'wb', compression, self)

    def write(self, b):
        self.buffer += b

    def transform(self, chunk):
        self.zipfile.write(chunk)
        r = self.buffer
        self.buffer = b''
        return r

    def close(self):
        self.zipfile.close()
        return self.buffer


@TransformerTubeFactory()
class Split(object):
    """
    Split splits source data on a delimiter.
    """

    def __init__(self, on=b'\n'):
        self.on = on
        self.buffer = b''  # read buffer

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


@TransformerTubeFactory()
class Joined(object):
    """
    Joined does single level flattening of streams.
    """

    def __init__(self, by=b""):
        self.by = by
        self.first = True

    def transform(self, chunk):
        if self.first:
            self.first = False
            return self.by.join(chunk)
        else:
            return self.by + self.by.join(chunk)


@TransformerTubeFactory(OBJ_CHUNK_SIZE)
class JSONLoads(object):
    """
    JSONLoads is not very smart. It expects a stream of complete raw
    JSON byte strings and works well with Delimiter for source files with one
    JSON object per line.
    """

    def __init__(self, encoding='utf-8'):
        self.encoding = encoding

    def transform(self, chunk):
        raws = filter(None, chunk)
        return [json.loads(raw.decode(self.encoding)) for raw in raws]


@TransformerTubeFactory(OBJ_CHUNK_SIZE)
class JSONDumps(object):
    """
    JSONDumps takes an object stream and serializes it to an
    array of json byte strings.
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


@TransformerTubeFactory(OBJ_CHUNK_SIZE)
class ChunkMap(object):
    """
    ChunkMap turns a chunk mapper function into a TubeFactory.
    """

    def __init__(self, fn, close_fn=None, abort_fn=None):
        self.fn = fn
        self.close_fn = close_fn
        self.abort_fn = abort_fn

    def transform(self, chunk):
        return self.fn(chunk)

    def close(self):
        return self.close_fn and self.close_fn()

    def abort(self):
        return self.abort_fn and self.abort_fn()


@TransformerTubeFactory(OBJ_CHUNK_SIZE)
class Map(object):
    """
    Map takes a callback and applies it to each
    element of the chunk. It's the easiest way to make a transformer.
    """

    def __init__(self, fn, close_fn=None, abort_fn=None):
        self.fn = fn
        self.close_fn = close_fn
        self.abort_fn = abort_fn

    def transform(self, chunk):
        return map(self.fn, chunk)

    def close(self):
        return self.close_fn and self.close_fn()

    def abort(self):
        return self.abort_fn and self.abort_fn()


@TransformerTubeFactory()
class Tee(object):
    """
    Tee writes the chunk to the specified sink and passes it along
    the apparatus.
    """

    def __init__(self, sink):
        self.sink = sink
        self.result = None

    def transform(self, chunk):
        self.sink.write(chunk)
        return chunk

    def close(self):
        self.result = self.sink.result()


@TransformerTubeFactory(OBJ_CHUNK_SIZE)
class Filter(object):
    """
    Filter takes a filter function and wraps a call to the filter
    built-in for each chunk.
    """

    def __init__(self, fn):
        self.fn = fn

    def transform(self, chunk):
        return list(filter(self.fn, chunk))


@TransformerTubeFactory(OBJ_CHUNK_SIZE)
class Noop(object):
    """
    Noop is useful for buffering. Set chunksize for upstream
    sinks.
    """
    def transform(self, chunk):
        return chunk
