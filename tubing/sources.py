from __future__ import print_function
"""
Tubing sources are defined here. If you want to make your own Source, create a
reader class with a read(amt) function, where amt is the amount of `stuff` to
read. MakeSource can generate a Source from your Reader. Ex::

    class MyReader(object):
        def read(self, amt):
            return [0] * amt, False

    MySource = MakeSource(MyReader)

The read(amt) function should return a chunk of data and a boolean indicated if
we've reached EOF. Unlike normal python streams, it's ok to return empty sets
and it won't close the stream. Only returning True as the second parameter
indicates that the stream is closed.
"""
import logging

logger = logging.getLogger('tubing.sources')


class MakeSource(object):
    """
    MakeSource takes a reader object and returns a Source factory.
    """

    def __init__(self, reader_cls):
        self.reader_cls = reader_cls

    def __call__(self, *args, **kwargs):
        return Source(self.reader_cls(*args, **kwargs))


class Source(object):
    """
    Source is a wrapper for Readers that allows piping.
    """

    def __init__(self, source):
        self.source = source

    def read(self, amt):
        return self.source.read(amt)

    def __or__(self, other):
        return self.pipe(other)

    def pipe(self, other):
        return other(self)


class ObjectReader(object):
    """
    ObjectReader outputs a list of objects.
    """

    def __init__(self, objs):
        self.objs = objs

    def read(self, amt=None):
        r, self.objs = self.objs[:amt], self.objs[amt or len(self.objs):]
        return r, len(self.objs) == 0


Objects = MakeSource(ObjectReader)
