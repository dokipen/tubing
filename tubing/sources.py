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
from requests import Session, Request
from tubing import compat

logger = logging.getLogger('tubing.sources')

HANDLERS = []

def handle_signals(*signals):
    def handle(*args, **kwargs):
        for handle in HANDLERS:
            handle()
    for sig in signals:
        signal.signal(sig, handle)

handle_signals(signal.SIGTERM, signal.SIGINT, signal.SIGHUP)

class MakeSourceFactory(object):
    """
    MakeSourceFactory takes a reader object and returns a Source factory.
    """

    def __init__(self, reader_cls):
        self.reader_cls = reader_cls

    def __call__(self, *args, **kwargs):
        reader = self.reader_cls(*args, **kwargs)
        src = Source(reader)
        if hasattr(reader, 'interrupt'):
            HANDLERS.append(reader.interrupt)
        return src


@compat.python_2_unicode_compatible
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
        return other.receive(self)

    def __str__(self):
        return "<tubing.sources.Source(%s)>" % (self.source)


class ObjectReader(object):
    """
    ObjectReader outputs a list of objects.
    """

    def __init__(self, objs):
        self.objs = objs

    def read(self, amt=None):
        r, self.objs = self.objs[:amt], self.objs[amt or len(self.objs):]
        return r, len(self.objs) == 0


Objects = MakeSourceFactory(ObjectReader)


class FileReader(object):
    """
    FileReader outputs bytes.
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


File = MakeSourceFactory(FileReader)


class SocketReader(object):
    """
    UDPReader binds and reads from a UDP socket.
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
            logger.debug("%s >> %s" % (addr, data))
            return data, False
        except socket.timeout:
            return b'', False
        except socket.error:
            return b'', True

    def interrupt(self):
        self.eof = True


Socket = MakeSourceFactory(SocketReader)


class BytesReader(object):
    def __init__(self, byts):
        self.io = io.BytesIO(byts)

    def read(self, amt=None):
        r = self.io.read(amt)
        if r:
            return r, False
        else:
            return b'', True

Bytes = MakeSourceFactory(BytesReader)


class IOReader(object):
    def __init__(self, stream):
        self.io = stream

    def read(self, amt=None):
        r = self.io.read(amt)
        if r:
            return r, False
        else:
            return b'', True

IO = MakeSourceFactory(IOReader)


class HTTPReader(object):
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

HTTP = MakeSourceFactory(HTTPReader)
