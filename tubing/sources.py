from __future__ import print_function
"""
Tubing sources are defined here.
"""
import logging
import io


logger = logging.getLogger('tubing.sources')


class Objects(object):
    """
    Outputs a list of objects.
    """
    def __init__(self, objs):
        self.objs = objs

    def read(self, amt=None):
        r, self.objs = self.objs[:amt], self.objs[amt or len(self.objs):]
        logger.debug("Returning {}".format(r))
        return r

    def __or__(self, pipe):
        return pipe(self)
