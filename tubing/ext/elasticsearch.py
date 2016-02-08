from __future__ import print_function
"""
Elasticsearch Extension.
"""
import json
import requests
import logging
from tubing import sinks, pipes

logger = logging.getLogger('tubing.ext.elasticsearch')


class DocUpdate(object): # pragma: no cover
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

    def __init__(self, base, index_name, username=None, password=None, encoding='utf-8'):
        self.base = base
        self.index_name = index_name
        self.username = username
        self.password = password
        self.encoding = encoding

    def _url(self):
        return "%s/%s/_bulk" % (self.base, self.index_name)

    def _auth(self):
        if self.username or self.password:
            return self.username, self.password

    def write(self, chunk):
        data = b''
        for update in chunk:
            data += update.serialize(self.encoding)
        logger.debug("POSTING: " + data)
        resp = requests.post(self._url(), data=data, auth=self._auth())
        logger.debug(resp.text)
        resp_obj = json.loads(resp.text)
        if resp_obj['errors']:
            raise ElasticSearchError(resp.text)


BulkSink = sinks.MakeSink(BulkSinkWriter, default_chunk_size=2 ** 10)
