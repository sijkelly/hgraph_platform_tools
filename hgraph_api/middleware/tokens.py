"""
Signed bearer tokens for the OMS API.

Tokens are compact JWTs (RFC 7519) signed with HMAC-SHA256 using a shared
secret (``API_AUTH_SECRET``).  They are produced by :func:`create_token`
(exposed on the CLI as ``hgraph-tools api-token``) or by any issuer that
shares the secret, and verified by :func:`verify_token`.

Only the ``HS256`` algorithm is accepted; the ``alg`` header is checked
explicitly so ``none`` or asymmetric-algorithm confusion cannot be used.
"""

import base64
import hashlib
import hmac
import json
import time

__all__ = (
    "MIN_SECRET_LENGTH",
    "TokenError",
    "create_token",
    "verify_token",
)

MIN_SECRET_LENGTH = 32

# Tolerance for clock skew between issuer and API server, in seconds.
_LEEWAY_SECONDS = 30


class TokenError(ValueError):
    """Raised when a bearer token is malformed, badly signed or expired."""


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(segment: str) -> bytes:
    padding = "=" * (-len(segment) % 4)
    try:
        return base64.urlsafe_b64decode(segment + padding)
    except (ValueError, TypeError) as exc:
        raise TokenError("Malformed token encoding") from exc


def _sign(signing_input: bytes, secret: str) -> bytes:
    return hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()


def _check_secret(secret: str) -> None:
    if len(secret) < MIN_SECRET_LENGTH:
        raise ValueError(f"API_AUTH_SECRET must be at least {MIN_SECRET_LENGTH} characters")


def create_token(user_id: str, secret: str, *, ttl_seconds: int = 8 * 3600, now: float | None = None) -> str:
    """Create a signed token identifying ``user_id``.

    :param user_id: The user the token authenticates (stored as the ``sub`` claim).
    :param secret: Shared HMAC secret (``API_AUTH_SECRET``).
    :param ttl_seconds: Lifetime of the token in seconds.
    :param now: Override for the current time (for tests).
    :returns: Compact JWT string.
    """
    _check_secret(secret)
    if not user_id:
        raise ValueError("user_id must not be empty")
    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds must be positive")

    issued_at = int(now if now is not None else time.time())
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {"sub": user_id, "iat": issued_at, "exp": issued_at + ttl_seconds}

    signing_input = ".".join(
        _b64url_encode(json.dumps(part, separators=(",", ":")).encode("utf-8")) for part in (header, payload)
    ).encode("ascii")
    signature = _b64url_encode(_sign(signing_input, secret))
    return f"{signing_input.decode('ascii')}.{signature}"


def verify_token(token: str, secret: str, *, now: float | None = None) -> str:
    """Verify a token and return the authenticated user id.

    :param token: Compact JWT string.
    :param secret: Shared HMAC secret (``API_AUTH_SECRET``).
    :param now: Override for the current time (for tests).
    :returns: The ``sub`` claim.
    :raises TokenError: If the token is malformed, has a bad signature or has expired.
    """
    _check_secret(secret)
    parts = token.split(".")
    if len(parts) != 3:
        raise TokenError("Malformed token")
    header_seg, payload_seg, signature_seg = parts

    try:
        header = json.loads(_b64url_decode(header_seg))
        payload = json.loads(_b64url_decode(payload_seg))
    except json.JSONDecodeError as exc:
        raise TokenError("Malformed token payload") from exc
    if not isinstance(header, dict) or not isinstance(payload, dict):
        raise TokenError("Malformed token payload")

    if header.get("alg") != "HS256":
        raise TokenError("Unsupported token algorithm")

    expected = _sign(f"{header_seg}.{payload_seg}".encode("ascii"), secret)
    if not hmac.compare_digest(expected, _b64url_decode(signature_seg)):
        raise TokenError("Invalid token signature")

    current = now if now is not None else time.time()
    exp = payload.get("exp")
    if not isinstance(exp, (int, float)) or current > exp + _LEEWAY_SECONDS:
        raise TokenError("Token expired")
    nbf = payload.get("nbf")
    if isinstance(nbf, (int, float)) and current < nbf - _LEEWAY_SECONDS:
        raise TokenError("Token not yet valid")

    sub = payload.get("sub")
    if not isinstance(sub, str) or not sub:
        raise TokenError("Token has no subject")
    return sub
