from __future__ import print_function
"""
This is where sources are connected to sinks.
"""
import logging


logger = logging.getLogger('tubing.pipe')


def pipe(source, sink):
    try:
        for doc in source.readall():
            sink.post(doc)

        sink.done()
    except:
        logger.exception("Failed pipe")
        sink.abort()
        return "FAILED"

    return "OK"
