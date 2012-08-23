"""
Base functionality for the Ganeti RAPI, client-side.

This module provides combinators which are used to provide a full RAPI client.
"""

from base64 import b64encode
import simplejson as json
from urllib import urlencode

from twisted.internet import reactor
from twisted.internet.defer import (Deferred, DeferredList, inlineCallbacks,
                                    succeed)
from twisted.internet.error import ConnectionRefusedError
from twisted.internet.protocol import Protocol
from twisted.python import log
from twisted.web.client import Agent, HTTPConnectionPool
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer
from zope.interface import implements

from gentleman.errors import ClientError, GanetiApiError, NotOkayError
from gentleman.helpers import prepare_query

_headers = Headers({
    "accept": ["application/json"],
    "content-type": ["application/json"],
    "user-agent": ["Ganeti RAPI Client (Twisted)"],
})


class StringProducer(object):
    implements(IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


class JsonResponseProtocol(Protocol):

    def __init__(self, d):
        self._upstream = d
        self._finished = Deferred()
        self.buf = []

    def getData(self):
        dl = DeferredList([self._finished, self._upstream],
                          fireOnOneErrback=True)

        @dl.addCallback
        def cb(l):
            return l[0][1]

        @dl.addErrback
        def eb(fail):
            return fail.value.subFailure

        return dl

    def dataReceived(self, data):
        self.buf.append(data)

    def connectionLost(self, reason):
        try:
            data = json.loads("".join(self.buf))
        except Exception, e:
            self._finished.errback(e)
        else:
            self._finished.callback(data)


class TwistedRapiClient(object):
    """
    Ganeti RAPI client using Twisted's Agent for HTTP.
    """

    _json_encoder = json.JSONEncoder(sort_keys=True)

    version = None
    features = []

    def __init__(self, host, port=5080, username=None, password=None,
                 timeout=60):
        """
        Initializes this class.

        @type host: string
        @param host: the ganeti cluster master to interact with
        @type port: int
        @param port: the port on which the RAPI is running (default is 5080)
        @type username: string
        @param username: the username to connect with
        @type password: string
        @param password: the password to connect with
        """

        if username is not None and password is None:
            raise ClientError("Password not specified")
        elif password is not None and username is None:
            raise ClientError("Specified password without username")

        self.headers = _headers.copy()

        if username and password:
            encoded = b64encode("%s:%s" % (username, password))
            self.headers.addRawHeader("Authorization", "Basic %s" % encoded)

        pool = HTTPConnectionPool(reactor, persistent=True)
        self._agent = Agent(reactor, connectTimeout=timeout, pool=pool)

        self._base_url = "https://%s:%d" % (host, port)


    def request(self, method, path, query=None, content=None):
        """
        Sends an HTTP request.

        This constructs a full URL, encodes and decodes HTTP bodies, and
        handles invalid responses in a pythonic way.

        @type method: string
        @param method: HTTP method to use
        @type path: string
        @param path: HTTP URL path
        @type query: list of two-tuples
        @param query: query arguments to pass to urllib.urlencode
        @type content: str or None
        @param content: HTTP body content

        @rtype: object
        @return: JSON-Decoded response

        @raises GanetiApiError: If an invalid response is returned
        """

        if not path.startswith("/"):
            raise ClientError("Implementation error: Called with bad path %s"
                              % path)

        body = None

        if content is not None:
            data = self._json_encoder.encode(content)
            body = StringProducer(data)

        url = self._base_url + path

        if query:
            prepare_query(query)
            params = urlencode(query, doseq=True)
            url += "?%s" % params

        log.msg("Sending request to %s %s %s" % (url, self.headers, body),
                system="Gentleman")

        d = self._agent.request(method, url, headers=self.headers,
                                bodyProducer=body)

        protocol = JsonResponseProtocol(d)

        @d.addErrback
        def connectionFailed(failure):
            failure.trap(ConnectionRefusedError)
            raise GanetiApiError("Connection refused!")

        @d.addCallback
        def cb(response):
            if response.code != 200:
                raise NotOkayError(code=response.code)
            response.deliverBody(protocol)

        return protocol.getData()


    @staticmethod
    def applier(f, d):
        d.addCallback(f)
        return d


    @inlineCallbacks
    def start(self):
        """
        Confirm that we may access the target cluster.
        """

        version = yield self.request("get", "/version")

        if version != 2:
            raise GanetiApiError("Can't work with Ganeti RAPI version %d" %
                                 version)

        log.msg("Accessing Ganeti RAPI, version %d" % version,
                system="Gentleman")
        self.version = version

        try:
            features = yield self.request("get", "/2/features")
        except NotOkayError, noe:
            if noe.code == 404:
                # Okay, let's calm down, this is totally reasonable. Certain
                # older Ganeti RAPIs don't have a list of features.
                features = []
            else:
                # No, wait, panic was the correct thing to do.
                raise

        log.msg("RAPI features: %r" % (features,), system="Gentleman")
        self.features = features
