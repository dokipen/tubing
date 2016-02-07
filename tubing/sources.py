from __future__ import print_function
"""
Tubing sources are defined here.
"""
import logging


logger = logging.getLogger('tubing.sources')


class MakeSource(object):
    def __init__(self, reader_cls):
        self.reader_cls = reader_cls

    def __call__(self, *args, **kwargs):
        return Source(self.reader_cls(*args, **kwargs))


class Source(object):
    def __init__(self, source):
        self.source = source

    def read(self, amt):
        return self.source.read(amt)

    def __or__(self, other):
        return other(self)


class ObjectReader(object):
    """
    Outputs a list of objects.
    """
    def __init__(self, objs):
        self.objs = objs

    def read(self, amt=None):
        r, self.objs = self.objs[:amt], self.objs[amt or len(self.objs):]
        return r, len(self.objs) == 0


Objects = MakeSource(ObjectReader)
