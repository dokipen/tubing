.. image:: https://imgur.com/Q9Lv0xo.png
     :target: https://github.com/dokipen/tubing

======

.. image:: https://travis-ci.org/dokipen/tubing.svg?branch=master
    :target: https://travis-ci.org/dokipen/tubing/
.. image:: https://coveralls.io/repos/github/dokipen/tubing/badge.svg?branch=master
    :target: https://coveralls.io/github/dokipen/tubing?branch=master
.. image:: https://img.shields.io/pypi/v/tubing.svg
    :target: https://pypi.python.org/pypi/tubing/
.. image:: https://img.shields.io/pypi/pyversions/tubing.svg
    :target: https://pypi.python.org/pypi/tubing/
.. image:: https://img.shields.io/pypi/dd/tubing.svg
    :target: https://pypi.python.org/pypi/tubing/
.. image:: https://img.shields.io/pypi/l/tubing.svg
    :target: https://pypi.python.org/pypi/tubing/
.. image:: https://img.shields.io/pypi/wheel/tubing.svg
    :target: https://pypi.python.org/pypi/tubing/

Tubing is a Python I/O library.  What makes tubing so freakin' cool is the
gross abuse of the bitwise OR operator (|). Have you ever been writing python
code and thought to yourself, "Man, this is great, but I really wish it was a
little more like bash." Welp, we've made python a little more like bash.If you
are a super lame nerd-kid, you can replace any of the bitwise ORs with the
pipe() function and pray we don't overload any other operators in future
versions. If you do avoid the bitwise OR, we don't know if we want to hang out
with you.

Tubing is pretty bare-bones at the moment. We've tried to make it easy to add
your own functionality. Hopefully you find it not all that unpleasant. There
are three sections below for adding sources, pipes and sink. If you do make
some additions, think about committing them back upstream. We'd love to have
a full suite of tools.

Now, witness the full power of this fully operational I/O library.

.. code-block:: python

    from tubing import sources, pipes, sinks

    objs = [
        dict(
            name="Bob Corsaro",
            birthdate="08/03/1977",
            alignment="evil",
        ),
        dict(
            name="Tom Brady",
            birthdate="08/03/1977",
            alignment="good",
        ),
    ]
    sources.Objects(objs) \
         | pipes.JSONSerializer() \
         | pipes.Joined(by=b"\n") \
         | pipes.Gzip() \
         | sinks.File("output.gz", "wb")

Then in our old friend bash.

.. code-block:: bash

    $ zcat output.gz
    {"alignment": "evil", "birthdate": "08/03/1977", "name": "Bob Corsaro"}
    {"alignment": "good", "birthdate": "08/03/1977", "name": "Tom Brady"}
    $

We need to seriously think about renaming pipes to tubes.. man, what was I
thinking?

Catalog
-------

- sources
  - `sources.Objects`, takes a `list` of python objects.
- pipes
  - `pipes.Gunzip`, unzips a binary stream.
  - `pipes.Gzip`, zips a binary stream.
  - `pipes.JSONParser`, parses a byte string stream of raw JSON objects.
  - `pipes.JSONSerializer`, serializes an object stream using `json.dumps`.
  - `pipes.Split`, splits a stream that supports the `split` method.
  - `pipes.Joined`, joins a stream of the same type as the `by` argument.
  - `pipes.Debugger`, proxies stream, writing each chunk to the tubing.pipes debugger with level DEBUG.
- sinks
  - `sinks.Bytes`, saves each chunk self.results.
  - `sinks.File`, writes each chunk to a file.
  - `sinks.Debugger`, writes each chunk to the tubing.pipes debugger with level DEBUG.

Sources
-------

To make your own source, create a Reader class with the following interface.

.. code-block:: python

    class MyReader(object):
        """
        MyReader returns count instances of data.
        """
        def __init__(self, data="hello world\n", count=10):
            self.data = data
            self.count = count

        def read(self, amt):
            """
            read(amt) returns $amt of data and a boolean indicating EOF.
            """
            if not amt:
                amt = self.count
            r = self.data * min(amt, self.count)
            self.count -= amt
            return r, self.count <= 0

The important thing to remember is that your read function should return an
iterable of units of data, not a single piece of data. Then wrap your reader in
the loving embrace of MakeSource.

.. code-block:: python

    from tubing import sources

    MySource = sources.MakeSource(MyReader)

Now it can be used in a pipeline!

.. code-block:: python

    from __future__ import print_function

    from tubing import pipes
    sink = MySource(data="goodbye cruel world!", count=1) \
         | pipes.Joined(by=b"\n") \
         | sinks.Bytes()

    print(sinks.result)
    # Output: goodby cruel world!

Pipes
-----

Making your own pipe is a lot more fun, trust me. First make a Transformer.

.. code-block:: python

    class OptimusPrime(object):
        def transform(self, chunk):
            return list(reversed(chunk))

`chunk` is an iterable with a len() of whatever type of data the stream is
working with. In Transformers, you don't need to worry about buffer size or
closing or exception, just transform an iterable to another iterable. There are
lots of examples in pipes.py.

Next give Optimus Prime a hug.

.. code-block:: python

    from tubing import pipes

    AllMixedUp = pipes.MakePipe(OptimusPrime)

Ready to mix up some data?

.. code-block:: python

    from __future__ import print_function

    import json
    from tubing import sources, sinks

    objs = [{"number": i} for i in range(0, 10)]

    sink = sources.Objects(objs) \
         | AllMixedUp(chunk_size=2) \
         | sinks.Objects()

    print(json.dumps(sink))
    # Output: [{"number": 1}, {"number": 0}, {"number": 3}, {"number": 2}, {"number": 5}, {"number": 4}, {"number": 7}, {"number": 6}, {"number": 9}, {"number": 8}]

Sinks
-----

Really getting tired of making documentation... Maybe I'll finish later. I have real work to do.

Well.. I'm this far, let's just push through.

.. code-block:: python

    from __future__ import print_function
    from tubing import sources, pipes, sinks

    class StdoutWriter(object):
        def write(self, chunk):
            for part in chunk:
                print(part)

        def close(self):
            # this function is optional
            print("That's all folks!")

        def abort(self):
            # this is also optional
            print("Something terrible has occurred.")

    Debugger = sinks.MakeSink(StdoutWriter)

    objs = [{"number": i} for i in range(0, 10)]

    sink = sources.Objects(objs) \
         | AllMixedUp(chunk_size=2) \
         | pipes.JSONSerializer() \
         | pipes.Joined(by=b"\n") \
         | Debugger()
    # Output:
    #{"number": 1}
    #{"number": 0}
    #{"number": 3}
    #{"number": 2}
    #{"number": 5}
    #{"number": 4}
    #{"number": 7}
    #{"number": 6}
    #{"number": 9}
    #{"number": 8}
    #That's all folks!
