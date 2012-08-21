"""
Common errors.
"""

class GentleError(Exception):
    """
    Root of all errors in Gentleman.
    """


class CertificateError(GentleError):
    """
    There is a problem with an SSL certificate.
    """


class GanetiApiError(GentleError):
    """
    There was some sort of problem with the RAPI.
    """

class NotOkayError(GanetiApiError):
    """
    Specifically, we received a response from the RAPI that is not okay.
    """

    def __init__(self, code=None, *args, **kwargs):
        super(NotOkayError, self).__init__(*args, **kwargs)
        self.code = code
