"""
Base functionality for the Ganeti RAPI, client-side.

This module provides combinators which are used to provide a full RAPI client.
"""

import simplejson as json
import socket

import requests

from gentleman.errors import GanetiApiError


GANETI_RAPI_PORT = 5080
GANETI_RAPI_VERSION = 2

REPLACE_DISK_PRI = "replace_on_primary"
REPLACE_DISK_SECONDARY = "replace_on_secondary"
REPLACE_DISK_CHG = "replace_new_secondary"
REPLACE_DISK_AUTO = "replace_auto"

NODE_EVAC_PRI = "primary-only"
NODE_EVAC_SEC = "secondary-only"
NODE_EVAC_ALL = "all"

NODE_ROLE_DRAINED = "drained"
NODE_ROLE_MASTER_CANDIATE = "master-candidate"
NODE_ROLE_MASTER = "master"
NODE_ROLE_OFFLINE = "offline"
NODE_ROLE_REGULAR = "regular"

JOB_STATUS_QUEUED = "queued"
JOB_STATUS_WAITING = "waiting"
JOB_STATUS_CANCELING = "canceling"
JOB_STATUS_RUNNING = "running"
JOB_STATUS_CANCELED = "canceled"
JOB_STATUS_SUCCESS = "success"
JOB_STATUS_ERROR = "error"
JOB_STATUS_FINALIZED = frozenset([
  JOB_STATUS_CANCELED,
  JOB_STATUS_SUCCESS,
  JOB_STATUS_ERROR,
  ])
JOB_STATUS_ALL = frozenset([
  JOB_STATUS_QUEUED,
  JOB_STATUS_WAITING,
  JOB_STATUS_CANCELING,
  JOB_STATUS_RUNNING,
  ]) | JOB_STATUS_FINALIZED

# Legacy name
JOB_STATUS_WAITLOCK = JOB_STATUS_WAITING

# Internal constants
_REQ_DATA_VERSION_FIELD = "__version__"
_INST_NIC_PARAMS = frozenset(["mac", "ip", "mode", "link"])
_INST_CREATE_V0_DISK_PARAMS = frozenset(["size"])
_INST_CREATE_V0_PARAMS = frozenset([
    "os", "pnode", "snode", "iallocator", "start", "ip_check", "name_check",
    "hypervisor", "file_storage_dir", "file_driver", "dry_run",
])
_INST_CREATE_V0_DPARAMS = frozenset(["beparams", "hvparams"])

# Feature strings
INST_CREATE_REQV1 = "instance-create-reqv1"
INST_REINSTALL_REQV1 = "instance-reinstall-reqv1"
NODE_MIGRATE_REQV1 = "node-migrate-reqv1"
NODE_EVAC_RES1 = "node-evac-res1"

# Old feature constant names in case they're references by users of this module
_INST_CREATE_REQV1 = INST_CREATE_REQV1
_INST_REINSTALL_REQV1 = INST_REINSTALL_REQV1
_NODE_MIGRATE_REQV1 = NODE_MIGRATE_REQV1
_NODE_EVAC_RES1 = NODE_EVAC_RES1

headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "user-agent": "Ganeti RAPI Client",
}


def prepare_query(query):
    """
    Prepare a query object for the RAPI.

    RAPI has lots of curious rules for coercing values.

    This function operates on dicts in-place and has no return value.

    @type query: dict
    @param query: Query arguments
    """

    for name in query:
        value = query[name]

        # None is sent as an empty string.
        if value is None:
            query[name] = ""

        # Booleans are sent as 0 or 1.
        elif isinstance(value, bool):
            query[name] = int(value)

        # XXX shouldn't this just check for basestring instead?
        elif isinstance(value, dict):
            raise ValueError("Invalid query data type %r" %
                             type(value).__name__)


class RequestsRapiClient(object):
    """
    Ganeti RAPI client using the requests library as its backend.
    """

    _json_encoder = json.JSONEncoder(sort_keys=True)

    def __init__(self, host, port=GANETI_RAPI_PORT, username=None,
                 password=None, timeout=60):
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


    def _SendRequest(self, method, path, query=None, content=None):
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
            raise GanetiApiError(str(r.status_code), code=r.status_code)

        if r.content:
            return json.loads(r.content)
        else:
            return None


    def get(self, *args, **kwargs):
        return self._SendRequest("get", *args, **kwargs)
