from __future__ import print_function
"""
Elasticsearch Extension.
"""
import json
import requests
import logging
from tubing import sinks


logger = logging.getLogger('tubing.ext.elasticsearch')


class BulkUpdateBatcherSink(sinks.ProxySink):
    """
    Creates bulk updates for Elastic Search.
    This is an object stream handler and expects objects with
    index() and to_dict() methods implemented.
    """
    def __init__(self, sink, batch_size=7500):
        self.sink = sink
        self.batch = []
        self.batch_size = batch_size

    def action(self, doc):
        return json.dumps(dict(
            update=dict(
                _id=doc.index(),
            ),
        ))

    def update(self, doc):
        return json.dumps(dict(
            doc_as_upsert=True,
            doc=doc.to_dict(),
        ))

    def write(self, doc):
        self.batch.append("{}\n{}\n".format(self.action(doc), self.update(doc)))
        if len(self.batch) > self.batch_size:
            batch = ''.join(self.batch[:self.batch_size])
            self.batch = self.batch[self.batch_size:]
            self.sink.write(batch)

    def done(self):
        self.sink.write(''.join(self.batch))
        return super(BulkUpdateBatcherSink, self).done()


class BulkSink(sinks.BaseSink):
    """
    Elastic search bulk writer.
    """
    def __init__(self, base, index_name, doc_type, username, password):
        self.base = base
        self.index_name = index_name
        self.doc_type = doc_type
        self.username = username
        self.password = password

    def _url(self):
        return "{}/{}/{}/_bulk".format(self.base, self.index_name, self.doc_type)

    def _auth(self):
        return self.username, self.password

    def write(self, chunk):
        logger.debug("POSTING: {}".format(chunk))
        resp = requests.post(self._url(), data=chunk, auth=self._auth())
        resp_obj = json.loads(resp.text)
        logger.debug(json.dumps(resp_obj, indent=2))
        # TODO: handle errors
