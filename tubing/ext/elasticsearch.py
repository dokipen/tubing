from __future__ import print_function
"""
Elasticsearch Extension.
"""
import json
import requests
import logging
from tubing import sinks, pipes

logger = logging.getLogger('tubing.ext.elasticsearch')


class DocUpdate(object):
    """
    DocUpdate is an ElasticSearch document update object. It is meant to be
    used with BulkBatcher and returns an action and update.
    """

    def __init__(
        self,
        esid,
        doc,
        doc_type,
        parent_esid=None,
        doc_as_upsert=True
    ):
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

    def action(self, encoding):
        action = dict(update=dict(_id=self.esid, _type=self.doc_type,),)
        if self.parent_esid:
            action['update']['parent'] = self.parent_esid
        return json.dumps(action).encode(encoding)

    def update(self, encoding):
        return json.dumps(
            dict(
                doc_as_upsert=self.doc_as_upsert,
                doc=self.doc,
            )
        ).encode(encoding)

    def serialize(self, encoding):
        return self.action(encoding) + b"\n" + self.update(encoding) + b"\n"


class BulkBatcherTransform(object):
    """
    BulkBatcherTransform creates bulk updates for DocUpdates.  This is an
    object stream handler and expects DocUpdate objects.
    """

    def __init__(self, bulk_batch_size=2**10, encoding='utf-8'):
        self.batch = []
        self.batch_size = bulk_batch_size
        self.encoding = encoding

    def transform(self, docs):
        for doc in docs:
            self.batch.append(doc.serialize(self.encoding))
        if len(self.batch) > self.batch_size:
            batch = b''.join(self.batch[:self.batch_size])
            self.batch = self.batch[self.batch_size:]
            return batch
        else:
            return b''

    def close(self):
        logger.debug("done called")
        return b''.join(self.batch)


BulkBatcher = pipes.MakePipe(BulkBatcherTransform)


class ElasticSearchError(Exception):
    """
    ElasticSearchError message is the text response from the elasticsearch
    server.
    """
    pass


class BulkSinkWriter(object):  # pragma: no cover
    """
    BulkSinkWriter writes bulk updates to Elastic Search. If username or
    password is None, then auth will be skipped.
    """

    def __init__(self, base, index_name, username=None, password=None):
        self.base = base
        self.index_name = index_name
        self.username = username
        self.password = password

    def _url(self):
        return "%s/%s/_bulk" % (self.base, self.index_name)

    def _auth(self):
        if self.username or self.password:
            return self.username, self.password

    def write(self, chunk):
        logger.debug("POSTING: " + chunk)
        resp = requests.post(self._url(), data=chunk, auth=self._auth())
        logger.debug(resp.text)
        resp_obj = json.loads(resp.text)
        if resp_obj['errors']:
            raise ElasticSearchError(resp.text)


BulkSink = sinks.MakeSink(BulkSinkWriter)