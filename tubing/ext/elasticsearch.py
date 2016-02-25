from __future__ import print_function
"""
Elasticsearch Extension.
"""
import json
import logging
from tubing import sinks, tubes

logger = logging.getLogger('tubing.ext.elasticsearch')


class DocUpdate(object):  # pragma: no cover
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


class BulkUpdateTransformer(object):
    """
    BulkSinkWriter writes bulk updates to Elastic Search. If username or
    password is None, then auth will be skipped.
    """

    def __init__(self, encoding='utf-8'):
        self.encoding = encoding

    def transform(self, chunk):
        data = []
        for update in chunk:
            data.append(update.serialize(self.encoding))
        return data


PrepareBulkUpdate = tubes.MakeTransformerTubeFactory(
    BulkUpdateTransformer,
    default_chunk_size=2**10
)


def BulkUpdate(
    base_url,
    index,
    username=None,
    password=None,
    chunk_size=50,
    chunks_per_post=20,
    fail_on_error=True,
):
    """
    Docs per post is chunk_size * chunks_per_post.
    """
    url = "%s/%s/_bulk" % (base_url, index)

    def response_handler(resp):
        try:
            try:
                resp_obj = json.loads(resp.text)
            except ValueError:
                raise ElasticSearchError("invalid response: '%s'" % (resp.text))

            if resp_obj['errors']:
                raise ElasticSearchError(
                    "errors in response: '%s'" % (resp.text)
                )
        except:
            logger.exception("%s %s", resp, resp.text)
            if fail_on_error:
                raise

    return sinks.HTTPPost(
        url=url,
        username=username,
        password=password,
        chunk_size=chunk_size,
        chunks_per_post=chunks_per_post,
        response_handler=response_handler,
    )
