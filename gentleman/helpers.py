"""
Some utility units.
"""

from operator import itemgetter

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

def itemgetters(*args):
    """
    Get a handful of items from an iterable.

    This is just map(itemgetter(...), iterable) with a list comprehension.
    """

    f = itemgetter(*args)

    def inner(l):
        return [f(x) for x in l]

    return inner
