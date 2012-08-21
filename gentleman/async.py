"""
Base functionality for the Ganeti RAPI, client-side.

This module provides combinators which are used to provide a full RAPI client.
"""

import simplejson as json

from twisted.internet import reactor
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet.error import ConnectionRefusedError
from twisted.internet.protocol import Protocol
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from twisted.python import log

from gentleman.errors import ClientError, GanetiApiError, NotOkayError
from gentleman.helpers import prepare_query

headers = Headers({
    "accept": ["application/json"],
    "content-type": ["application/json"],
    "user-agent": ["Ganeti RAPI Client (Twisted)"],
})


class JsonResponseProtocol(Protocol):

    def __init__(self):
        self.d = Deferred()
        self.buf = []

    def dataReceived(self, data):
        self.buf.append(data)

    def connectionLost(self, reason):
        try:
            data = json.loads("".join(self.buf))
        except Exception, e:
            self.d.errback(e)
        else:
            self.d.callback(data)


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
        @param logger: Logging object
        """

        if username is not None and password is None:
            raise ClientError("Password not specified")
        elif password is not None and username is None:
            raise ClientError("Specified password without username")

        self._agent = Agent(reactor, connectTimeout=timeout)

        self.username = username
        self.password = password

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

        kwargs = {
            "verify": False,
        }

        if self.username and self.password:
            kwargs["auth"] = self.username, self.password

        if content is not None:
            kwargs["data"] = self._json_encoder.encode(content)

        if query:
            prepare_query(query)
            kwargs["params"] = query

        url = self._base_url + path

        log.msg("Sending request to %s %s" % (url, kwargs))

        d = self._agent.request(method, url, headers=headers,
                                bodyProducer=None)

        protocol = JsonResponseProtocol()

        @d.addErrback
        def connectionFailed(failure):
            failure.trap(ConnectionRefusedError)
            raise GanetiApiError("Connection refused!")

        @d.addCallback
        def cb(response):
            if response.code != 200:
                raise NotOkayError(code=response.code)
            response.deliverBody(protocol)

        return protocol.d

    @inlineCallbacks
    def _start(self):
        """
        Confirm that we may access the target cluster.
        """

        version = yield self.request("/version")

        if version != 2:
            raise GanetiApiError("Can't work with Ganeti RAPI version %d" %
                                 version)

        log.msg("Accessing Ganeti RAPI, version %d" % version)
        self.version = version

        try:
            features = yield self.request("/2/features")
        except NotOkayError, noe:
            if noe.code == 404:
                # Okay, let's calm down, this is totally reasonable. Certain
                # older Ganeti RAPIs don't have a list of features.
                features = []
            else:
                # No, wait, panic was the correct thing to do.
                raise

        log.msg("RAPI features: %r" % (features,))
        self.features = features

    def start(self):
        return self._start()
