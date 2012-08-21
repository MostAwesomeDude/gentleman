"""
Base functionality for the Ganeti RAPI, client-side.

This module provides combinators which are used to provide a full RAPI client.
"""

import logging
import simplejson as json
import socket

import requests

from gentleman.errors import ClientError, GanetiApiError, NotOkayError
from gentleman.helpers import prepare_query

headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "user-agent": "Ganeti RAPI Client (Requests)",
}


class RequestsRapiClient(object):
    """
    Ganeti RAPI client using the requests library as its backend.
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

        self.username = username
        self.password = password
        self.timeout = timeout

        try:
            socket.inet_pton(socket.AF_INET6, host)
            address = "[%s]:%s" % (host, port)
        # ValueError can happen too, so catch it as well for the IPv4
        # fallback.
        except (socket.error, ValueError):
            address = "%s:%s" % (host, port)

        self._base_url = "https://%s" % address


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

        kwargs = {
            "headers": headers,
            "timeout": self.timeout,
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

        # print "Sending request to %s %s" % (url, kwargs)

        try:
            r = requests.request(method, url, **kwargs)
        except requests.ConnectionError:
            raise GanetiApiError("Couldn't connect to %s" % self._base_url)
        except requests.Timeout:
            raise GanetiApiError("Timed out connecting to %s" %
                                 self._base_url)

        if r.status_code != requests.codes.ok:
            raise NotOkayError(str(r.status_code), code=r.status_code)

        if r.content:
            return json.loads(r.content)
        else:
            return None


    @staticmethod
    def applier(f, a):
        return f(a)


    def start(self):
        """
        Confirm that we may access the target cluster.
        """

        version = self.request("get", "/version")

        if version != 2:
            raise GanetiApiError("Can't work with Ganeti RAPI version %d" %
                                 version)

        logging.info("Accessing Ganeti RAPI, version %d" % version)
        self.version = version

        try:
            features = self.request("get", "/2/features")
        except NotOkayError, noe:
            if noe.code == 404:
                # Okay, let's calm down, this is totally reasonable. Certain
                # older Ganeti RAPIs don't have a list of features.
                features = []
            else:
                # No, wait, panic was the correct thing to do.
                raise

        logging.info("RAPI features: %r" % (features,))
        self.features = features
