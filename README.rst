=========
Gentleman
=========

Gentleman is a simple, straightforward Python library for communication with
Ganeti clusters using Ganeti's RAPI.

Usage
=====

Import a client of your choice, and then do some work with it.

    >>> from gentleman.sync import RequestsRapiClient
    >>> c = RequestsRapiClient("your.ganeti.cluster")
    >>> c.start()
    >>> print c.version
    2
    >>> print c.features
    ['instance-reinstall-reqv1', 'node-evac-res1', 'node-migrate-reqv1',
    'instance-create-reqv1']

There's also a Twisted client. An example with Twisted's shell:

    >>> from gentleman.async import *
    >>> c = TwistedRapiClient("33.33.33.10")
    >>> c.start()
    <Deferred #0>
    Deferred #0 called back: None
    >>> c.version
    2
    >>> c.features
    ['instance-reinstall-reqv1', 'node-evac-res1', 'node-migrate-reqv1',
    'instance-create-reqv1']

License
=======

Gentleman is made available under the terms of the GPL, version 2 or (at your
discretion) any later version.

Gentleman is based on code from Ganeti, (c) 2010-11 Google Inc., and code from
Ganeti Web Manager, (c) 2011-12 Oregon State University.
