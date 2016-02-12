from __future__ import print_function
"""
This is where all transformations in the pipe occur. To make your own pipe,
define a Transformer object and pass it to MakePipe.
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
    """
    MakePipe is called on a Transformer class to create a Pipe factory for the
    Transformer. default_chunk_size is meant to create a sensible default read
    amt value, depending on the type of stream. 2 ** 16 objects can be a lot
    bigger than 2 ** 16 bytes. It's a good idea to specify. Here's an example::

        class MyTransformer(object):
            def transform(self, chunk):
                return reversed(chunk)

        MyPipe = MakePipe(MyTransformer, default_chunk_size=10)

    A Transformer can also implement close() and/or abort() functions. close()
    is called when the upstream source hits EOF and is allowed to return a
    final chunk of data.

    There are lots more examples below.
    """

    def __init__(self, transformer_cls, default_chunk_size=2**16):
        self.transformer_cls = transformer_cls
        self.default_chunk_size = default_chunk_size

    def __call__(self, *args, **kwargs):
        chunk_size = self.default_chunk_size
        if kwargs.get("chunk_size"):
            chunk_size = kwargs["chunk_size"]
            del kwargs["chunk_size"]

        def fn(source):
            transformer = self.transformer_cls(*args, **kwargs)
            return Pipe(source, chunk_size, transformer)

        return fn


class PipeIterator(object):

    def __init__(self, pipe, chunk_size):
        self.pipe = pipe
        self.chunk_size = chunk_size
        self.eof = False

    def next(self):
        if self.eof:
            logger.debug("iter stopped")
            raise StopIteration

        r, self.eof = self.pipe.read(self.chunk_size)
        logger.debug("iter >> {}".format(r))
        return r

    def __next__(self):
        """
        Python 3 support
        """
        return self.next()

    def __iter__(self):
        return self


class Pipe(object):
    """
    Pipe wraps a Transformer and does all the grunt work that most pipes need
    to do.  Transformers should implement transform(chunk), and optionally
    close() and abort().
    """

    def __init__(self, source, chunk_size, transformer):
        self.source = source
        self.chunk_size = chunk_size
        self.transformer = transformer
        self.eof = False
        self.buffer = None

    def __or__(self, other):
        return self.pipe(other)

    def pipe(self, other):
        return other(self)

    def read_complete(self, amt):
        """
        read_complete tells us if the current request is fulfilled. It's fulfilled
        if we've reached the EOF in the source, or we have $amt parts. If amt is
        None, we should read to the source's EOF.
        """
        return self.eof or amt and self.buffer and len(self.buffer) >= amt

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
        if self.buffer:
            self.buffer += chunk
        else:
            self.buffer = chunk

    def buffer_len(self):
        """
        buffer_len even if buffer is None.
        """
        return self.buffer and len(self.buffer) or 0

    def read(self, amt=None):
        """
        This is where the rubber meets the snow.
        """
        try:
            while not self.read_complete(amt):
                inchunk, self.eof = self.source.read(self.chunk_size)
                if inchunk:
                    outchunk = self.transformer.transform(inchunk)
                    if outchunk:
                        self.append(outchunk)
                if self.eof and hasattr(self.transformer, 'close'):
                    self.append(self.transformer.close())

            if self.eof and (not amt or self.buffer_len() <= amt):
                # We've written everything, we're done
                return self.shift_buffer(amt), True

            return self.shift_buffer(amt), False
        except:
            logger.exception("Pipe failed")
            hasattr(self.transformer, 'abort') and self.transformer.abort()
            raise

    def read_iterator(self, chunk_size):
        return PipeIterator(self, chunk_size)

    def gen(self, chunk_size):
        while True:
            r, eof = self.read(chunk_size)
            yield r
            if eof:
                return


class GunzipTransformer(object):
    """
    GunzipTransformer unzips a gzipped source stream.
    """

    def __init__(self):
        self.dec = zlib.decompressobj(32 + zlib.MAX_WBITS)

    def transform(self, chunk):
        return self.dec.decompress(chunk) or b''


Gunzip = MakePipe(GunzipTransformer)


class GzipTransformer(object):
    """
    GzipTransformer Gzips the binary input.
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


Gzip = MakePipe(GzipTransformer)


class SplitTransformer(object):
    """
    SplitTransformer splits source data on a delimiter.
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


class JSONParserTransformer(object):
    """
    JSONParserTransformer is not very smart. It expects a stream of complete raw
    JSON byte strings and works well with Delimiter for source files with one
    JSON object per line.
    """

    def __init__(self, encoding='utf-8'):
        self.encoding = encoding

    def transform(self, chunk):
        raws = filter(None, chunk)
        return [json.loads(raw.decode(self.encoding)) for raw in raws]


JSONParser = MakePipe(JSONParserTransformer)


class JSONSerializerTransformer(object):
    """
    JSONSerializerTransformer takes an object stream and serializes it to an
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


JSONSerializer = MakePipe(JSONSerializerTransformer)


# Where should this all purpose thing go? Here I guess.
class DebugPrinter(object):

    def transform(self, chunk):
        logger.debug(chunk)
        return chunk


Debugger = MakePipe(DebugPrinter)


class TransformerTransformer(object):

    def __init__(self, callback):
        self.callback = callback

    def transform(self, chunk):
        return self.callback(chunk)


Transformer = MakePipe(TransformerTransformer)


class ObjectStreamTransformerTransformer(object):

    def __init__(self, callback):
        self.callback = callback

    def transform(self, chunk):
        r = []
        for obj in chunk:
            r.append(self.callback(obj))
        return r


ObjectStreamTransformer = MakePipe(ObjectStreamTransformerTransformer)
