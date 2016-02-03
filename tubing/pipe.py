from __future__ import print_function
"""
This is where sources are connected to sinks.
"""
import logging


logger = logging.getLogger('tubing.pipe')

from guppy import hpy
hp = hpy()

def pipe(source, sink, amt=None):
    logger.debug("Starting PIPE")
    try:
        chunk = source.read(amt)
        while chunk:
            sink.write(chunk)
            import gc; gc.collect()
            chunk = source.read(amt)

        sink.done()
    except:
        logger.exception("Failed pipe")
        sink.abort()
        raise

    return "OK"
