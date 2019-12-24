class GaApiError(Exception):
    """Base exception for API errors."""

class GaInvalidArgumentError(GaApiError):
    """Exception for errors on the report definition."""

class GaAuthenticationError(GaApiError):
    """Exception for UNAUTHENTICATED && PERMISSION_DENIED errors."""

class GaRateLimitError(GaApiError):
    """Exception for Rate Limit errors."""

class GaQuotaExceededError(GaApiError):
    """Exception for Quota Exceeded errors."""

class GaBackendServerError(GaApiError):
    """Exception for 500 and 503 backend errors that are Google's fault"""

class GaUnknownError(GaApiError):
    """Exception for unknown errors."""
