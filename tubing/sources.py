from __future__ import print_function
"""
Tubing sources are defined here. Here's an example of a gziped s3 document with
one JSON object per line::

    bucket = 'mybucket'
    key = 'path/to/myfile.gz'
    source = JSONParser(LineReaderSource(ZlibSource(S3Source(bucket, key))))

    for obj in iter(source.read, ''):
        # do something with obj dict
"""

import json
import logging
import zlib
import gzip


logger = logging.getLogger('tubing.sources')


class ZlibSource(object):
    """
    ZlibSource unzips a gzipped source stream.
    """
    def __init__(self, source):
        self.source = source
        self.dec = zlib.decompressobj(32 + zlib.MAX_WBITS)

    def read(self, amt=None):
        chunk = self.source.read(amt)
        return self.dec.decompress(chunk)


class LineReaderSource(object):
    """
    LineReaderSource allow readline on a source stream.
    """
    def __init__(self, source, chunksize=4096):
        self.source = source
        self.chunksize = chunksize
        self.buffer = []
        self.eof = False

    def read(self, amt=1):
        response = []
        for _ in range(0, amt):
            response.append(self.readline())
            if self.eof:
                break
        return response

    def readline(self):
        """
        Read a single line.
        """
        if self.eof:
            return ''

        output = []
        newbuffer = []
        state = "outputting"
        for chunk in self.buffer:
            if "\n" in chunk:
                out, buf = chunk.split("\n", 1)
                output.append(out)
                newbuffer.append(buf)
                state = "buffering"
            elif state == "outputting":
                output.append(chunk)
            else:
                newbuffer.append(chunk)

        self.buffer = newbuffer
        while state == "outputting":
            chunk = self.source.read(self.chunksize)
            if not chunk:
                # EOF
                self.eof = True
                return ''.join(output)
            if "\n" in chunk:
                out, buf = chunk.split("\n", 1)
                output.append(out)
                self.buffer.append(buf)
                state = "buffering"
            else:
                output.append(chunk)

        return ''.join(output)


class JSONParserSource(object):
    """
    JSONParserSource is not very smart. It expects one raw JSON object per read
    and works well with LineReaderSource for source files with one JSON object
    per line.
    """
    def __init__(self, source):
        self.source = source

    def read(self, amt=1):
        response = []
        for line in self.source.read(amt):
            if line:
                response.append(json.loads(line.strip()))
        return response

    def readobj(self):
        return self.read()[0]
