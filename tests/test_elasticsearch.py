import json
import unittest2 as unittest
from tubing.ext import elasticsearch
from tubing import sinks, sources

EXPECTED = [
    {
        "update": {
            "_id": "id0",
            "_type": "test"
        }
    }, {
        "doc": {"name": "id0"},
        "doc_as_upsert": True
    }, {
        "update": {
            "_id": "id1",
            "_type": "test-child",
            "parent": "id0"
        }
    }, {
        "doc": {"name": "id1"},
        "doc_as_upsert": False
    }
]


class ElasticSearchTestCase(unittest.TestCase):

    def test_es(self):
        payload = [
            elasticsearch.DocUpdate(
                'id0',
                dict(name='id0'),
                'test',
            ),
            elasticsearch.DocUpdate(
                'id1',
                dict(name='id1'),
                'test-child',
                parent_esid='id0',
                doc_as_upsert=False,
            ),
        ]

        sink = sources.Objects(payload) \
             | elasticsearch.BulkBatcher() \
             | sinks.Bytes()

        f = []
        for line in sink.result.split(b'\n'):
            if line:
                f.append(json.loads(line.decode('utf-8')))

        self.assertEqual(EXPECTED, f)

    def test_batching(self):

        def make_du(i):
            esid = 'id%d' % i
            return elasticsearch.DocUpdate(
                esid=esid,
                doc=dict(name=esid),
                doc_type='test',
            )

        sink = sources.Objects([make_du(i) for i in range(0,150)]) \
             | elasticsearch.BulkBatcher(bulk_batch_size=50) \
             | sinks.Bytes()

        self.assertEqual(301, len(sink.result.split(b"\n")))
