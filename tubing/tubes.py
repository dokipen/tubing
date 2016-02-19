from __future__ import print_function
"""
This is where all transformations in the tube occur. To make your own tube,
define a Transformer object and pass it to MakeTransformerTubeFactory.

There are two types of users. Irish users, and users who wish they were Irish.
j/k. Really, they are:

 - plebe users want to use whatever we have in tubing.
 - noble users who want to extend tubing to their own devices.
 - royal users want to contribute to tubing.

We are socialists, so we want to take care of the plebes, first and formost.
They are also the easiest to satisfy. For them, we have tubes. Tubes are easy
to use and understand.

Nobles are very important too, but harder to satisfy. We never know what crazy
plans they'll have in mind, so we must be ready. They need the tools to build
new tubes that extend to infinity and beyond.

For the benefit of the nobles, and ourselves, we're going to outline exactly
how things work now, and possibly make them simpler in the process.

The easiest way to extend tubing is to create a Transformer, and use MakeTransformerTubeFactory
to turn it into a Tube. A Transformer has the following iterface::

    class Transformer(object):
        def transform(self, chunk):
            return new_chunk

    NewPipe = MakePipe(Transformer)

A chunk is an iterable of whatever type of stream we are working on, whether it
be bytes, unicode characters, strings or python objects.  We can index it,
slice it, or iterate over it. `transform` simple takes a chunk, and makes a new
chunk out of it. `MakePipe` will take care of all the dirty work. Transformers
are enough for most tasks, but if you need to do something more complex, you
may need to go deeper.

..image:: http://i.imgur.com/DyPouyL.png
    alt: Leonardo Decaprio

First let's describe how tubes work in more detail. Here's the Tube iterface::

    class TubeFactory(object):
        # This is what we export, and what is called when users create a tube.
        # The syntax looks like this:
        # SourceFactory() | [TubeFactory()...] | SinkFactory()
        def __call__(self, *args, **kwargs):
            return Tube()

    # - or -

    def TubeFactory(*args, **kwargs):
        return Tube()

    class Tube(object):
        def receive(self, source):
            # return a TubeWorker
            tw = TubeWorker
            tw.source = source
            return tw

    class TubeWorker(object):
        def tube(self, reciever):
            # reciever is they guy who will call our `read` method. Either
            # another Tube or a Sink.
            return reciever.receive(self)

        def __or__(self, *args, **kwargs):
            # Our reason for existing.
            return self.tube(*args, **kwargs)

        def read(self, amt=None):
            # our reciever will call this guy. We return a tuple here of
            # `chunk, eof`.  We should return a chunk of len amt of whatever
            # type of object we produce. If we've exhausted our upstream
            # source, then we should return True as the second element of our
            # tuple.
            return [], True

A TubeFactory is what plebes deal with. As you can see, it can be an object or
a function, depending on your style. It's easier for me to reason about state
with an object, but if you prefer a closure, go for it! Classes are just
closures with more verbose states.

When a plebe is setting up some tubing, the TubeFactory returns a Tube, but this
isn't the last object we'll create. The Tube doesn't have a source connected, so
it's sort of useless. It's just a placeholder waiting for a source. As soon as
it gets a source, it will hand off all of it's duties to a TubeWorker.

A TubeWorker is ready to read from it's source, but it doesn't. TubeWorkers are
pretty lazy and need someone else to tell them what to do. That's where a receiver
comes in hand. A reciever can be another Tube, or a Sink.  If it's another Tube,
you know the drill. It's just another lazy guy that will only tell his source to
read when his boss tells him to read. Ultimately, the only guy who wants to do
any work is the Sink. At the end of the chain, a sink's recieve function will
be called, and he'll get everyone to work.

Technically, we could split the TubeWorker interface into two parts, but it's not
really necessary since they share the same state. We could also combine TubeFactory,
Tube and TubeWorker, and just build up state overtime. I've seriously consider this,
but I don't know, this feels better. I admit, it is a little complicated.

MakeTransformerTubeFactory
--------------------------

So how does MakeTransformerTubeFactoryFactory turn a Transformer into a TubeFactory?
MakeTransformerTubeFactoryFactory is a utility that creates a function that wrap a transformer in
a tube. Sort of complicate, eh? I'm sorry about that, but let's see if we can
break it down.

MakeTransformerTubeFactoryFactory returns a partial function out of the TransformerTube
instantiation. For the uninitiated, a partial is just a new version
of a function with some of the parameters already filled in. So we're currying
the transformer_cls and the default_chunk_size back to the plebes. They
can fill in the rest of the details and get back a TransformerTube.

The TransformerTubeWorker is where most of the hard work happens. There's a
bunch of code related to reading just enough chunks from our source to satisfy
our reciever. Remeber, Workers are lazy, that's good.

default_chunk_size is sort of important, by default it's something like 32k. It's
the size of the chunks that we request from upstream, in the read function (amt).
That's great for byte streams(maybe?), but it's not that great for large objects.
You'll probably want to set it if you are using something other than bytes. It
can be overriden by plebes, this is just the default if they don't specify it.
Remember, we should be making the plebes job easy, so try and be a nice noble
and set it to something sensible.


"""
import logging
import json
import zlib
import gzip
import functools

logger = logging.getLogger('tubing.tubes')

DEBUG = False


def MakeTransformerTubeFactory(transformer_cls, default_chunk_size=2**16):
    return functools.partial(TransformerTube, transformer_cls, default_chunk_size)


class TransformerTube(object):
    def __init__(self, transformer_cls, default_chunk_size, *args, **kwargs):
        self.chunk_size = default_chunk_size
        if kwargs.get("chunk_size"):
            self.chunk_size = kwargs["chunk_size"]
            del kwargs["chunk_size"]

        self.transformer_cls = transformer_cls
        self.args = args
        self.kwargs = kwargs

    def recieve(self, source):
        transformer = self.transformer_cls(*self.args, **self.kwargs)
        return TransformerTubeWorker(source, self.chunk_size, transformer)


class TransformerTubeWorker(object):
    """
    Tube wraps a Transformer and does all the grunt work that most tubes need
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
        return self.tube(other)

    def tube(self, other):
        return other.recieve(self)

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
            logger.exception("Tube failed")
            hasattr(self.transformer, 'abort') and self.transformer.abort()
            raise

    def read_iterator(self, chunk_size):
        return TubeIterator(self, chunk_size)

    def gen(self, chunk_size):
        while True:
            r, eof = self.read(chunk_size)
            yield r
            if eof:
                return


class TubeIterator(object):

    def __init__(self, tube, chunk_size):
        self.tube = tube
        self.chunk_size = chunk_size
        self.eof = False

    def next(self):
        if self.eof:
            logger.debug("iter stopped")
            raise StopIteration

        r, self.eof = self.tube.read(self.chunk_size)
        logger.debug("iter >> %s" % (r))
        return r

    def __next__(self):
        """
        Python 3 support
        """
        return self.next()

    def __iter__(self):
        return self


class GunzipTransformer(object):
    """
    GunzipTransformer unzips a gzipped source stream.
    """

    def __init__(self):
        self.dec = zlib.decompressobj(32 + zlib.MAX_WBITS)

    def transform(self, chunk):
        return self.dec.decompress(chunk) or b''


Gunzip = MakeTransformerTubeFactory(GunzipTransformer)


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


Gzip = MakeTransformerTubeFactory(GzipTransformer)


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


Split = MakeTransformerTubeFactory(SplitTransformer)


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


Joined = MakeTransformerTubeFactory(JoinedTransformer, 2**10)


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


JSONParser = MakeTransformerTubeFactory(JSONParserTransformer)


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


JSONSerializer = MakeTransformerTubeFactory(JSONSerializerTransformer)


# Where should this all purpose thing go? Here I guess.
class DebugPrinter(object):

    def transform(self, chunk):
        logger.debug(chunk)
        return chunk


Debugger = MakeTransformerTubeFactory(DebugPrinter)


class TransformerTransformer(object):

    def __init__(self, callback):
        self.callback = callback

    def transform(self, chunk):
        return self.callback(chunk)


Transformer = MakeTransformerTubeFactory(TransformerTransformer)


class ObjectStreamTransformerTransformer(object):

    def __init__(self, callback):
        self.callback = callback

    def transform(self, chunk):
        r = []
        for obj in chunk:
            r.append(self.callback(obj))
        return r


ObjectStreamTransformer = MakeTransformerTubeFactory(ObjectStreamTransformerTransformer)


class TeeWorker(object):
    def __init__(self, uuid, manager):
        self.uuid = uuid
        self.manager = manager

    def read(self, amt):
        return self.manager.read(uuid, amt)

    def tube(self, other):
        return other(self)

    def __or__(self, other):
        return self.tube(other)


class TeeManager(object):

    def __init__(self, source, chunk_size):
        self.source = source
        self.chunk_size = chunk_size
        self.eof = False
        self.buffers = {}

    def tee(self):
        uuid = uuid.uuid4()
        self.buffers[uuid] = None
        return TeeWorker(uuid, self)

    def read_complete(self, uuid, amt):
        """
        read_complete tells us if the current request is fulfilled. It's fulfilled
        if we've reached the EOF in the source, or we have $amt parts. If amt is
        None, we should read to the source's EOF.
        """
        return self.eof or amt and  self.buffer_len(uuid) >= amt

    def shift_buffers(self, uuid, amt):
        """
        Remove $amt data from the front of the buffers and return it.
        """
        if self.buffers[uuid]:
            r, self.buffers[uuid] = self.buffers[uuid][:amt], self.buffers[uuid][
                amt or len(
                    self.buffers[uuid]
                ):
            ]
            return r
        else:
            return b''

    def append(self, chunk):
        """
        append to the buffer, creating it if it doesn't exist.
        """
        for buffer in buffers.values():
            if self.buffer:
                self.buffer += chunk
            else:
                self.buffer = chunk

    def buffer_len(self, uuid):
        """
        buffer_len even if buffer is None.
        """
        return self.buffers[uuid] and len(self.buffers[uuid]) or 0

    def read(self, uuid, amt=None):
        """
        This is where the rubber meets the snow.
        """
        try:
            while not self.read_complete(uuid, amt):
                inchunk, self.eof = self.source.read(self.chunk_size)
                if inchunk:
                    outchunk = self.transformer.transform(inchunk)
                    if outchunk:
                        self.append(outchunk)

            if self.eof and (not amt or self.buffer_len() <= amt):
                # We've written everything, we're done
                return self.shift_buffer(uuid, amt), True

            return self.shift_buffer(uuid, amt), False
        except:
            logger.exception("Tube failed")
            raise


class TeeTransformer(object):
    def __init__(self, sink):
        self.sink = sink

    def transform(self, chunk):
        self.sink.write(chunk)
        return chunk


Tee = MakeTransformerTubeFactory(TeeTransformer)