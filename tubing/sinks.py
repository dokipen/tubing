from __future__ import print_function
"""
Tubing sinks are targets for streams of data.
"""

import json
import gzip
import logging
try:
    from StringIO import StringIO
except:         # pragma: no cover
    from io import StringIO
from io import BytesIO


logger = logging.getLogger('tubing.sinks')


def gen_fn(cls):
    def gen(chunk_size=2 ** 8, *args, **kwargs):
        def fn(source):
            sink = cls(*args, **kwargs)
            chunk = source.read(chunk_size)
            sink.write(chunk)
            while chunk:
                chunk = source.read(chunk_size)
                sink.write(chunk)
            return sink
        return fn
    return gen


class ObjectsSink(list):
    def write(self, objs):
        self.extend(objs)

Objects = gen_fn(ObjectsSink)
