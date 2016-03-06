"""
There are two types of tubing users. Irish users, and users who wish they were
Irish.  j/k. Really, they are:

 - casual users want to use whatever we have in tubing.
 - advanced users who want to extend tubing to their own devices.
 - contributor users want to contribute to tubing.

Our most important users are the casual users.  They are also the easiest to
satisfy. For them, we have tubes. Tubes are easy to use and understand.

Advanced users are very important too, but harder to satisfy. We never know
what crazy plans they'll have in mind, so we must be ready. They need the tools
to build new tubes that extend our apparatus in unexpected ways.

For the benefit of the contributors, and ourselves, we're going to outline exactly
how things work now. This documentation is also an exercise in understanding
and simplifying the code base.

We'll call a tubing pipeline an apparatus. An apparatus has a Source, zero to
many Tubes, and a Sink.

A stream is what we call the units flowing through our Tubes. The units can be
bytes, characters, strings or objects.

Tubes
=====

For most users, they simply want to transform elements of data as it goes through the stream. This can be achieved simply with the following idiom::

    SomeSource | [Tubes ..] | tubes.Map(lambda x: transform(x)) | [Tubes ..] | SomeSink

Sometimes you'd like to transform an entire chunk of data at a time, instead of one element at a time::

    SomeSource | [Tubes ..] | tubes.ChunkMap(lambda x: transform(x)) | [Tubes ..] | SomeSink

Other times you just want to filter out some data::

    SomeSource | [Tubes ..] | tubes.Filter(lambda x: x > 10) | [Tubes ..] | SomeSink

All of these general tube tools also take close_fn and abort_fn params and are
shorthand for creating your own Tube class.

Of course, if you need to keep state, you can create a closure, but at some point, that can become cumbersome. You might also want to make a reusable Tube, in that case it could be nice to make a

The easiest way to extend tubing is to create a Transformer, and use TransformerTubeFactory
decorator to turn it into a Tube. A Transformer has the following interface::

    @tubes.TransformerTubeFactory()
    class NewTube(object):
        def transform(self, chunk):
            return new_chunk

        def close(self):
            return last_chunk or None

        def abort(self):
            pass

A chunk is an iterable of whatever type of stream we are working on, whether it
be bytes, Unicode characters, strings or python objects.  We can index it,
slice it, or iterate over it. `transform` simple takes a chunk, and makes a new
chunk out of it. `TransformerTubeFactory` will take care of all the dirty
work. Transformers are enough for most tasks, but if you need to do something
more complex, you may need to go deeper.

.. image:: http://i.imgur.com/DyPouyL.png
    alt: Leonardo DiCaprio

First let's describe how tubes work in more detail. Here's the Tube interface::

    # tube factory can be a class

    class TubeFactory(object):
        # This is what we export, and what is called when users create a tube.
        # The syntax looks like this:
        # SourceFactory() | [TubeFactory()...] | SinkFactory()
        def __call__(self, *args, **kwargs):
            return Tube()

    #  or a function

    def TubeFactory(*args, **kwargs):
        return Tube()

    # ------------------------
    class Tube(object):
        def receive(self, source):
            # return a TubeWorker
            tw = TubeWorker
            tw.source = source
            return tw

    class TubeWorker(object):
        def tube(self, receiver):
            # receiver is they guy who will call our `read` method. Either
            # another Tube or a Sink.
            return receiver.receive(self)

        def __or__(self, *args, **kwargs):
            # Our reason for existing.
            return self.tube(*args, **kwargs)

        def read(self):
            # our receiver will call this guy. We return a tuple here of
            # `chunk, eof`.  We should return a chunk of len amt of whatever
            # type of object we produce. If we've exhausted our upstream
            # source, then we should return True as the second element of our
            # tuple. The chunk size should be configuratable and read should
            # return a len() of chunk size or less.
            return [], True

A TubeFactory is what casual users deal with. As you can see, it can be an
object or a function, depending on your style. It's easier for me to reason
about state with an object, but if you prefer a closure, go for it! Classes are
just closures with more verbose states, after all.

When a casual is setting up some tubing, the TubeFactory returns a Tube, but
this isn't the last object we'll create. The Tube doesn't have a source
connected, so it's sort of useless. It's just a placeholder waiting for a
source. As soon as it gets a source, it will hand off all of it's duties to a
TubeWorker.

A TubeWorker is ready to read from it's source, but it doesn't. TubeWorkers are
pretty lazy and need someone else to tell them what to do. That's where a receiver
comes in to play. A receiver can be another Tube, or a Sink.  If it's another
Tube, you know the drill. It's just another lazy guy that will only tell his
source to read when his boss tells him to read. Ultimately, the only guy who
wants to do any work is the Sink. At the end of the chain, a sink's receive
function will be called, and he'll get everyone to work.

Technically, we could split the TubeWorker interface into two parts, but it's
not really necessary since they share the same state. We could also combine
TubeFactory, Tube and TubeWorker, and just build up state overtime. I've
seriously consider this, but I don't know, this feels better. I admit, it is a
little complicated, but one advantage you get is that you can do something like
this::

    from tubing import tubes

    tube = tubes.GZip(chunk_size=2**32)

    source | tube | output1
    source | tube | output2

Since tube is a factory and not an object, each tubeline will have it's own
state. tubeline.... I just made that up. That's an execution pipeline in
tubing. But we don't want to call it a pipeline, it's a tubeline. Maybe there's
a better name? I picture a chemistry lab with a bunch of transparent tubes
connected to beakers and things like that.

.. image:: http://imgur.com/jTtHITH.jpg
    :alt: chemistry lab

Let's call it an apparatus.

TransformerTubeFactory
----------------------

So how does TransformerTubeFactory turn a Transformer into a TubeFactory?
TransformerTubeFactory is a utility that creates a function that wrap a
transformer in a tube. Sort of complicate, eh? I'm sorry about that, but let's
see if we can break it down.

TransformerTubeFactory returns a partial function out of the TransformerTube
instantiation. For the uninitiated, a partial is just a new version of a
function with some of the parameters already filled in. So we're currying the
transformer_cls and the default_chunk_size back to the casuals. They can fill
in the rest of the details and get back a TransformerTube.

The TransformerTubeWorker is where most of the hard work happens. There's a
bunch of code related to reading just enough chunks from our source to satisfy
our receiver. Remember, Workers are lazy, that's good because we won't waste a
bunch of space doing work we don't need to and then waiting for our work to
be consumed.

default_chunk_size is sort of important, by default it's something like 2**18.
It's the size of the chunks that we request from upstream, in the read function
(amt).  That's great for byte streams(maybe?), but it's not that great for
large objects.  You'll probably want to set it if you are using something other
than bytes. It can be overridden by plebes, this is just the default if they
don't specify it.  Remember, we should be making the plebes job easy, so try
and be a nice noble and set it to something sensible. In our own tests, using
2**3 for string or object streams and 2**18 for bytes streams seemed to give
the best trade off between speed and memory usage. YMMV.

We've explained Tubes, very well I might add. And it's a good thing. They are
the most complicated bit in tubing. All that's left is Sources and Sinks.

Sources
=======

TODO

Sinks
=====

TODO

Things You Can't do with Tubing
===============================

 - Tee to another apparatus
 - async programming
 - your laundry

"""
import pkgutil

try:
    version = pkgutil.get_data(__name__, 'VERSION').decode('utf-8')
except IOError:  # pragma: no cover
    version = '9999'

__version__ = version
