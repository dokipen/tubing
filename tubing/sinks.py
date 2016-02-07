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


class Objects(list):
    def __init__(self, amt=None):
        self.amt = amt

    def __call__(self, source):
        chunk = source.read(self.amt)
        self.extend(chunk)
        while chunk:
            chunk = source.read(self.amt)
            self.extend(chunk)
