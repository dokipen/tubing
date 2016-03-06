import logging
import unittest2 as unittest
from tubing import sinks, sources, tubes

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
        source = sources.Objects(SOURCE_DATA)
        apparatus = source | tubes.JSONDumps() \
                      | tubes.Joined(by=b"\n") \
                      | tubes.Gzip() \
                      | tubes.Gunzip() \
                      | tubes.Split(on=b"\n") \
                      | tubes.JSONLoads() \
                      | sinks.Objects()
        result = apparatus.result

        logger.debug("%r", result)
        self.assertEqual(result[0], SOURCE_DATA[0])
        self.assertEqual(result[1], SOURCE_DATA[1])
        self.assertEqual(result[2], SOURCE_DATA[2])
        self.assertEqual(result[3], SOURCE_DATA[3])

    def testFilter(self):

        def fn(line):
            return line["name"] == "Calvin"

        source = sources.Objects(SOURCE_DATA)
        apparatus = source | tubes.JSONDumps() \
                      | tubes.Joined(by=b"\n") \
                      | tubes.Gzip() \
                      | tubes.Gunzip() \
                      | tubes.Split(on=b"\n") \
                      | tubes.JSONLoads() \
                      | tubes.Filter(fn) \
                      | sinks.Objects()

        result = apparatus.result
        logger.debug("%r", result)
        self.assertEqual(result[0], SOURCE_DATA[3])

    def testFailingSink(self):
        results = dict(abort=False, close=False,)

        class FailSink(object):
            "Tom"

            def write(self, _):
                raise ValueError("Meant to fail")

            def close(self):
                results['close'] = True

            def abort(self):
                results['abort'] = True

        Fail = sinks.MakeSinkFactory(FailSink)

        class SucceedSink(object):
            "Bob"

            def write(self, _):
                pass

            def close(self):
                results['close'] = True

            def abort(self):
                results['abort'] = True

        Succeed = sinks.MakeSinkFactory(SucceedSink)

        try:
            sources.Objects(SOURCE_DATA) | Fail()
            self.assert_(False, "Expected Failure")
        except ValueError:
            pass

        self.assert_(results['abort'])
        self.assert_(not results['close'])

        results['close'] = False
        results['abort'] = False

        sources.Objects(SOURCE_DATA) | Succeed()
        self.assert_(not results['abort'])
        self.assert_(results['close'])
