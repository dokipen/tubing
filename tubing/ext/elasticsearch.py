from __future__ import print_function
"""
Elasticsearch Extension.
"""
import json
import requests
import logging
from tubing import sources, sinks, tubes

logger = logging.getLogger('tubing.ext.elasticsearch')


class DocUpdate(object):  # pragma: no cover
    """
    DocUpdate is an ElasticSearch document update object. It is meant to be
    used with BulkBatcher and returns an action and update.
    """

    def __init__(
        self,
        doc,
        doc_type,
        esid=None,
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
        if self.esid:
            action = dict(update=dict(_id=self.esid, _type=self.doc_type,),)
        else:
            action = dict(update=dict(_type=self.doc_type,),)

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


@tubes.TransformerTubeFactory(2**3)
class PrepareBulkUpdate(object):
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


def BulkUpdate(
    base_url,
    index,
    username=None,
    password=None,
    chunks_per_post=512,
    fail_on_error=True,
):
    """
    Docs per post is source.chunk_size * chunks_per_post.
    """
    url = "%s/%s/_bulk" % (base_url, index)

    def response_handler(resp):
        try:
            try:
                resp_obj = json.loads(resp.text)
            except ValueError:
                raise ElasticSearchError("invalid response: '%s'" % (resp.text))
            logger.debug(resp_obj)

            if resp_obj.get('errors'):
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
        chunks_per_post=chunks_per_post,
        response_handler=response_handler,
    )


@sources.SourceFactory()
class ScrollingSearch(object):

    def __init__(self, chunk_size, *args, **kwargs):
        self.chunk_size = chunk_size
        self.es = Scroller(*args, **kwargs)
        self.buffer = []
        self.finished = False

    def can_fill_read(self):
        return self.chunk_size <= len(self.buffer)

    def shift(self):
        s = self.chunk_size
        r, self.buffer = self.buffer[:s], self.buffer[s:]
        return r

    def scroll(self):
        hits = self.es.get_hits()
        if hits:
            self.buffer.extend(hits)
        else:
            self.finished = True

    def is_eof(self):
        return len(self.buffer) == 0 and self.finished

    def read(self):
        if self.is_eof():
            return [], True

        while not self.can_fill_read() and not self.finished:
            self.scroll()

        return self.shift(), self.is_eof()


class Scroller(object):

    def __init__(
        self,
        base_url,
        index,
        typ,
        query,
        timeout='10m',
        scroll_id=None,
        username=None,
        password=None,
    ):
        self.base_url = base_url
        self.init_endpoint = "{}/{}/_search".format(index, typ)
        self.scroll_endpoint = "/_search/scroll"
        self.timeout = timeout
        self.scroll_id = scroll_id
        self.query = query
        if username:
            self.auth = (username, password)
        else:
            self.auth = None

    def get_hits(self):
        if not self.scroll_id:
            endpoint = "{}{}?scroll={}&size=1000".format(
                self.base_url, self.init_endpoint, self.timeout
            )
            resp = requests.get(endpoint, json=self.query, auth=self.auth)
        else:
            endpoint = "{}{}".format(self.base_url, self.scroll_endpoint)
            body = dict(scroll=self.timeout, scroll_id=self.scroll_id)
            resp = requests.get(endpoint, json=body, auth=self.auth)

        self.scroll_id = resp.json().get('_scroll_id')
        if not self.scroll_id:
            raise ValueError("No scroll_id found in {}".format(
                json.dumps(resp.json(),
                           indent=2)))
        return resp.json().get('hits', {}).get('hits')
