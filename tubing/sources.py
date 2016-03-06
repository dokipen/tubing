from __future__ import print_function
"""
Tubing sources are defined here. If you want to make your own Source, create a
reader class with a read(amt) function, where amt is the amount of `stuff` to
read. MakeSourceFactory can generate a Source from your Reader. Ex::

    class MyReader(object):
        def read(self, amt):
            return [0] * amt, False

    MySource = MakeSourceFactory(MyReader)

The read(amt) function should return a chunk of data and a boolean indicated if
we've reached EOF. Unlike normal python streams, it's ok to return empty sets
and it won't close the stream. Only returning True as the second parameter
indicates that the stream is closed.
"""
import logging
import socket
import signal
import io
import sys
from requests import Session, Request
from tubing import compat, apparatus

logger = logging.getLogger('tubing.sources')

HANDLERS = []


def handle_signals(*signals):

    def handle(*args, **kwargs):
        try:
            for handle in HANDLERS:
                handle()
        except:
            logger.exception("Signal handlers failed")
            sys.exit(1)

        sys.exit(130)

    for sig in signals:
        signal.signal(sig, handle)


handle_signals(signal.SIGTERM, signal.SIGINT, signal.SIGHUP)


def SourceFactory(default_chunk_size=2**16):

    def wrapper(cls):
        return MakeSourceFactory(cls, default_chunk_size)

    return wrapper


class MakeSourceFactory(object):
    """
    MakeSourceFactory takes a reader object and returns a Source factory.
    """

    def __init__(self, reader_cls, default_chunk_size=2**16):
        self.reader_cls = reader_cls
        self.default_chunk_size = default_chunk_size

    def __call__(self, *args, **kwargs):
        chunk_size = self.default_chunk_size
        if kwargs.get("chunk_size"):
            chunk_size = kwargs["chunk_size"]
            del kwargs["chunk_size"]

        reader = self.reader_cls(*args, **kwargs)
        src = Source(reader, chunk_size)
        if hasattr(reader, 'interrupt'):
            HANDLERS.append(reader.interrupt)
        return src


@compat.python_2_unicode_compatible
class Source(object):
    """
    Source is a wrapper for Readers that allows piping.
    """

    def __init__(self, reader, chunk_size):
        self.reader = reader
        self.chunk_size = chunk_size
        self.app = None

    def read(self):
        logger.debug("[%s] Reading %s", self.reader, self.chunk_size)
        return self.reader.read(self.chunk_size)

    def __or__(self, other):
        return self.tube(other)

    def tube(self, other):
        if not self.app:
            # apparatus sets app on Source
            apparatus.Apparatus(self)
        return other.receive(self.app)

    def __str__(self):
        return "<tubing.readers.Source(%s)>" % (self.reader)


@SourceFactory(2**3)
class Objects(object):
    """
    Objects outputs a list of objects.
    """

    def __init__(self, objs):
        self.objs = objs

    def read(self, amt=None):
        r, self.objs = self.objs[:amt], self.objs[amt or len(self.objs):]
        return r, len(self.objs) == 0


@SourceFactory()
class File(object):
    """
    File outputs bytes.
    """

    def __init__(self, filename, mode="rb"):
        self.filename = filename
        self.f = open(self.filename, mode)

    def read(self, amt=None):
        chunk = self.f.read(amt)
        if chunk:
            return chunk, False
        else:
            return '', True

    def __unicode__(self):
        return u"<tubing.sources.File %s>" % (self.filename)

    def __str__(self):
        return unicode(self).encode('utf-8')


@SourceFactory()
class Socket(object):
    """
    Socket binds and reads from a socket.
    """

    def __init__(self, ip, port, *args):
        self.sock = socket.socket(*args)
        self.sock.bind((ip, port))
        self.sock.settimeout(0.1)
        self.eof = False

    def read(self, amt=None):
        if self.eof:
            return b'', True

        try:
            data, addr = self.sock.recvfrom(amt)
            logger.debug("%s >> %s", addr, data)
            return data, False
        except socket.timeout:
            return b'', False
        except socket.error:
            return b'', True

    def interrupt(self):
        self.eof = True


@SourceFactory()
class Bytes(object):

    def __init__(self, byts):
        self.io = io.BytesIO(byts)

    def read(self, amt=None):
        r = self.io.read(amt)
        if r:
            return r, False
        else:
            return b'', True


@SourceFactory()
class IO(object):

    def __init__(self, stream):
        self.io = stream

    def read(self, amt=None):
        r = self.io.read(amt)
        if r:
            return r, False
        else:
            return b'', True


@SourceFactory()
class HTTP(object):

    def __init__(self, method, url, *args, **kwargs):
        s = Session()
        s.stream = True
        r = Request(method, url, *args, **kwargs)
        self.stream = s.send(r.prepare()).raw

    def read(self, amt=None):
        r = self.stream.read(amt)
        if r:
            return r, False
        else:
            return b'', True
