import logging
import unittest2 as unittest
from tubing import sinks, sources, tubes, apparatus

SOURCE_DATA = [
    dict(
        name="Bob",
        age=38
    ),
    dict(
        name="Carrie",
        age=38
    ),
    dict(
        name="Devyn",
        age=18
    ),
    dict(
        name="Calvin",
        age=13
    ),
]

logger = logging.getLogger("tubing.test_tube")


class PipeTestCase(unittest.TestCase):

    def testPipe(self):
        app = apparatus.Apparatus(sources.Objects(SOURCE_DATA))
        app.connect(tubes.JSONDumps())
        app.connect(tubes.Joined(by=b"\n"))
        app.connect(tubes.Gzip())
        app.connect(tubes.Gunzip())
        app.connect(tubes.Split(on=b"\n"))
        app.connect(tubes.JSONLoads())
        app.connect(sinks.Objects())
        result = app.result

        logger.debug("%r", result)
        self.assertEqual(result[0], SOURCE_DATA[0])
        self.assertEqual(result[1], SOURCE_DATA[1])
        self.assertEqual(result[2], SOURCE_DATA[2])
        self.assertEqual(result[3], SOURCE_DATA[3])
