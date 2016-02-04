from __future__ import print_function
"""
Elasticsearch Extension.
"""
import json
import requests
import logging
from tubing import sinks


logger = logging.getLogger('tubing.ext.elasticsearch')


class DocUpdate(object):
    """
    DocUpdate is an ElasticSearch document update object. It is meant to be used with
    BulkBatcher and returns an action and update.
    """
    def __init__(self, esid, doc, doc_type, parent_esid=None, doc_as_upsert=True):
        """
        esid is an elasticsearch _id
        parent_esid is the elasticsearch _id of parent if parent exists
        doc is a dict
        """
        self.doc = doc
        self.esid = esid
        self.parent_esid = parent_esid
        self.doc_as_upsert = doc_as_upsert
        self.doc_type = doc_type

    def action(self):
        action = dict(
            update=dict(
                 _id=self.esid,
                 _type=self.doc_type,
            ),
        )
        if self.parent_esid:
            action['update']['parent'] = self.parent_esid
        return json.dumps(action)

    def update(self):
        return json.dumps(dict(
            doc_as_upsert=self.doc_as_upsert,
            doc=self.doc,
        ))



class BulkBatcherSink(sinks.ProxySink):
    """
    Creates bulk updates for Elastic Search.  This is an object stream handler
    and expects DocUpdate objects.
    """
    def __init__(self, sink, batch_size=7500):
        self.sink = sink
        self.batch = []
        self.batch_size = batch_size

    def write(self, docs):
        for doc in docs:
            self.batch.append("{}\n{}\n".format(doc.action(), doc.update()))
            if len(self.batch) > self.batch_size:
                batch = ''.join(self.batch[:self.batch_size])
                self.batch = self.batch[self.batch_size:]
                self.sink.write(batch)

    def done(self):
        self.sink.write(''.join(self.batch))
        return super(BulkBatcherSink, self).done()


class BulkSink(sinks.BaseSink): # pragma: no cover
    """
    Elastic search bulk writer.
    """
    def __init__(self, base, index_name, username, password):
        self.base = base
        self.index_name = index_name
        self.username = username
        self.password = password

    def _url(self):
        return "{}/{}/_bulk".format(self.base, self.index_name)

    def _auth(self):
        if self.username or self.password:
            return self.username, self.password

    def write(self, chunk):
        logger.debug("POSTING: {}".format(chunk))
        resp = requests.post(self._url(), data=chunk, auth=self._auth())
        resp_obj = json.loads(resp.text)
        logger.debug(json.dumps(resp_obj, indent=2))
        # TODO: handle errors
