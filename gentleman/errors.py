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
    The response from the RAPI was not OK.
    """

    def __init__(self, code=None, *args, **kwargs):
        super(GanetiApiError, self).__init__(*args, **kwargs)
        self.code = code
