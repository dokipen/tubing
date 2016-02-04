import unittest2 as unittest
import json
from tubing.ext import elasticsearch
from tubing import sinks, sources, pipe

EXPECTED = [{"update": {"_id": "id0", "_type": "test"}},
            {"doc": {"name": "id0"}, "doc_as_upsert": True},
            {"update": {"_id": "id1", "_type": "test-child", "parent": "id0"}},
            {"doc": {"name": "id1"}, "doc_as_upsert": False}]

class ElasticSearchTestCase(unittest.TestCase):
    def test_es(self):
        buffer0 =  sinks.BytesIOSink()
        sink = elasticsearch.BulkBatcherSink(buffer0)

        sink.write([elasticsearch.DocUpdate('id0', dict(name='id0'), 'test')])
        sink.write([elasticsearch.DocUpdate('id1', dict(name='id1'), 'test-child', parent_esid='id0', doc_as_upsert=False)])
        sink.done()

        buffer0.seek(0)
        f = []
        for line in buffer0.getvalue().split(b'\n'):
            if line:
                f.append(json.loads(line.decode('utf-8')))

        self.assertEqual(EXPECTED, f)

    def test_batching(self):
        buffer0 =  sinks.BytesIOSink()
        sink = elasticsearch.BulkBatcherSink(buffer0, batch_size=50)

        def make_du(i):
            esid = 'id%d'%i
            return elasticsearch.DocUpdate(
                esid=esid,
                doc=dict(name=esid),
                doc_type='test',
            )

        sink.write([make_du(i) for i in range(0,150)])
        sink.done()

        self.assertEqual(301, len(buffer0.getvalue().split(b"\n")))
