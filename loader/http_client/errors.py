class HTTPError(Exception):
    """Base class for HTTP errors."""

    pass


class MethodNotAllowedError(HTTPError):
    """Exception raised for method not allowed (HTTP 405)."""

    def __init__(self, message="Method Not Allowed"):
        self.message = message
        super().__init__(self.message)


class RateLimitReachedError(HTTPError):
    """Exception raised for rate limit reached (HTTP 429)."""

    def __init__(self, message="Rate Limit Reached"):
        self.message = message
        super().__init__(self.message)


class BadRequestError(HTTPError):
    """Exception raised for bad request (HTTP 400)."""

    def __init__(self, message="Bad Request"):
        self.message = message
        super().__init__(self.message)


class UnauthorizedError(HTTPError):
    """Exception raised for unauthorized access (HTTP 401)."""

    def __init__(self, message="Unauthorized"):
        self.message = message
        super().__init__(self.message)


class ForbiddenError(HTTPError):
    """Exception raised for forbidden access (HTTP 403)."""

    def __init__(self, message="Forbidden"):
        self.message = message
        super().__init__(self.message)


class NotFoundError(HTTPError):
    """Exception raised for resource not found (HTTP 404)."""

    def __init__(self, message="Not Found"):
        self.message = message
        super().__init__(self.message)


class InternalServerError(HTTPError):
    """Exception raised for internal server error (HTTP 500)."""

    def __init__(self, message="Internal Server Error"):
        self.message = message
        super().__init__(self.message)


class ServiceUnavailableError(HTTPError):
    """Exception raised for service unavailable (HTTP 503)."""

    def __init__(self, message="Service Unavailable"):
        self.message = message
        super().__init__(self.message)
