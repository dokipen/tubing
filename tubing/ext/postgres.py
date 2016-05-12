from __future__ import print_function
"""
Postgres Tubing extension.
"""

import psycopg2
import logging
from tubing import sources, sinks, compat

logger = logging.getLogger('tubing.ext.redshift')


@sources.SourceFactory(2**8)
@compat.python_2_unicode_compatible
class Query(object):  # pragma: no cover
    """
    Read file from S3. Expects AWS environmental variables to be set.
    """

    def __init__(self, connstr, query, as_dict=False):  # pragma: no cover
        """
        Create an S3 Source stream.
        """
        self.connstr = connstr
        conn = psycopg2.connect(connstr)
        self.cursor = conn.cursor()
        self.cursor.execute(query)
        self.as_dict = as_dict
        self.header =  map(lambda h: h.name, self.cursor.description)

    def read(self, amt):
        rows = self.cursor.fetchmany(amt)
        if self.as_dict:
            header = self.header
            def m(row):
                return dict(zip(self.header, row))
            rows = map(m, rows)
        if not rows:
            return rows, True
        else:
            return rows, None

    def __str__(self):
        return "<tubing.ext.postgres.Query %s>" % (self.connstr)
