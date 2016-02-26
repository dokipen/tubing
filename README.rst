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
.. image:: https://readthedocs.org/projects/tubing/badge/?version=latest
   :target: http://tubing.readthedocs.org/en/latest
.. image:: https://landscape.io/github/dokipen/tubing/master/landscape.svg?style=flat
   :target: https://landscape.io/github/dokipen/tubing/master
   :alt: Code Health

Tubing is a Python I/O library.  What makes tubing so freakin' cool is the
gross abuse of the bit-wise OR operator (|). Have you ever been writing python
code and thought to yourself, "Man, this is great, but I really wish it was a
little more like bash." Whelp, we've made python a little more like bash.If you
are a super lame nerd-kid, you can replace any of the bit-wise ORs with the
tube() function and pray we don't overload any other operators in future
versions. Here's how you install tubing::

    $ pip install tubing

Tubing is pretty bare-bones at the moment. We've tried to make it easy to add
your own functionality. Hopefully you find it not all that unpleasant. There
are three sections below for adding sources, tubes and sink. If you do make
some additions, think about committing them back upstream. We'd love to have
a full suite of tools.

Now, witness the power of this fully operational I/O library.

.. code-block:: python

    from tubing import sources, tubes, sinks

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
         | tubes.JSONSerializer() \
         | tubes.Joined(by=b"\n") \
         | tubes.Gzip() \
         | sinks.File("output.gz", "wb")

Then in our old friend bash.

.. code-block:: bash

    $ zcat output.gz
    {"alignment": "evil", "birthdate": "08/03/1977", "name": "Bob Corsaro"}
    {"alignment": "good", "birthdate": "08/03/1977", "name": "Tom Brady"}
    $

You can find more documentation on `readthedocs <https://tubing.readthedocs.org/>`_

Catalog
-------

Sources
~~~~~~~

+---------+-----------------------------------------------------+
|`Objects`|Takes a `list` of python objects.                    |
+---------+-----------------------------------------------------+
|`File`   |Creates a stream from a file.                        |
+---------+-----------------------------------------------------+
|`Bytes`  |Takes a byte string.                                 |
+---------+-----------------------------------------------------+
|`IO`     |Takes an object with a read function.                |
+---------+-----------------------------------------------------+
|`Socket` |Takes an addr, port and socket() args.              .|
+---------+-----------------------------------------------------+
|`HTTP`   |Takes an method, url and any args that can be passed |
|         |to requests library.                                 |
+---------+-----------------------------------------------------+

Tubes
~~~~~

+----------------+-----------------------------------------------------+
|`Gunzip`        |Unzips a binary stream.                              |
+----------------+-----------------------------------------------------+
|`Gzip`          |Zips a binary stream.                                |
+----------------+-----------------------------------------------------+
|`JSONParser`    |Parses a byte string stream of raw JSON objects.     |
+----------------+-----------------------------------------------------+
|`JSONSerializer`|Serializes an object stream using `json.dumps`.      |
+----------------+-----------------------------------------------------+
|`Split`         |Splits a stream that supports the `split` method.    |
+----------------+-----------------------------------------------------+
|`Joined`        |Joins a stream of the same type as the `by` argument.|
+----------------+-----------------------------------------------------+
|`Tee`           |Takes a sink and passes chunks along apparatus.      |
+----------------+-----------------------------------------------------+
|`Map`           |Takes a transformer function for single items in     |
|                |stream.                                              |
+----------------+-----------------------------------------------------+
|`Filter`        |Takes a filter test callback and only forwards items |
|                |that pass.                                           |
+----------------+-----------------------------------------------------+
|`ChunkMap`      |Takes a transformer function for batch of stream     |
|                |items.                                               |
+----------------+-----------------------------------------------------+
|`Debugger`      |Proxies stream, writing each chunk to the            |
|                |`tubing.tubes` debugger with level DEBUG.            |
+----------------+-----------------------------------------------------+

Sinks
~~~~~

+----------+----------------------------------------------------------------+
|`Objects` |A list that stores all passed items to self.                    |
+----------+----------------------------------------------------------------+
|`Bytes`   |Saves each chunk self.results.                                  |
+----------+----------------------------------------------------------------+
|`File`    |Writes each chunk to a file.                                    |
+----------+----------------------------------------------------------------+
|`HTTPPost`|Writes data via HTTPPost.                                       |
+----------+----------------------------------------------------------------+
|`Hash`    |Takes algorithm name, updates hash with contents.               |
+----------+----------------------------------------------------------------+
|`Debugger`|Writes each chunk to the tubing.tubes debugger with level DEBUG.|
+----------+----------------------------------------------------------------+

Extensions
~~~~~~~~~~

+-------------------------------------+-----------------------------------------------+
|`s3.S3Source`                        |Create stream from an S3 object.               |
+-------------------------------------+-----------------------------------------------+
|`s3.MultipartUploader`               |Stream data to S3 object.                      |
+-------------------------------------+-----------------------------------------------+
|`elasticsearch.BulkSink`             |Stream `elasticsearch.DocUpdate` objects to the|
|                                     |elasticsearch _bulk endpoint.                  |
+-------------------------------------+-----------------------------------------------+

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
the loving embrace of MakeSourceFactory.

.. code-block:: python

    from tubing import sources

    MySource = sources.MakeSourceFactory(MyReader)

Now it can be used in a apparatus!

.. code-block:: python

    from __future__ import print_function

    from tubing import tubes
    sink = MySource(data="goodbye cruel world!", count=1) \
         | tubes.Joined(by=b"\n") \
         | sinks.Bytes()

    print(sinks.result)
    # Output: goodbye cruel world!

Tubes
-----

Making your own tube is a lot more fun, trust me. First make a Transformer.

.. code-block:: python

    class OptimusPrime(object):
        def transform(self, chunk):
            return list(reversed(chunk))

`chunk` is an iterable with a len() of whatever type of data the stream is
working with. In Transformers, you don't need to worry about buffer size or
closing or exception, just transform an iterable to another iterable. There are
lots of examples in tubes.py.

Next give Optimus Prime a hug.

.. code-block:: python

    from tubing import tubes

    AllMixedUp = tubes.MakeTranformerTubeFactory(OptimusPrime)

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
    from tubing import sources, tubes, sinks

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

    Debugger = sinks.MakeSinkFactory(StdoutWriter)

    objs = [{"number": i} for i in range(0, 10)]

    sink = sources.Objects(objs) \
         | AllMixedUp(chunk_size=2) \
         | tubes.JSONSerializer() \
         | tubes.Joined(by=b"\n") \
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
