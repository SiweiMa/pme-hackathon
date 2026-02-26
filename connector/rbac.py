"""RBAC mapping: caller IAM ARN to denied KMS key aliases.

For the hackathon, this uses a static mapping from IAM role name substrings
to the set of KMS key aliases that should be denied (columns encrypted with
those keys are returned as nulls).

Production would use STS AssumeRole or IAM policy conditions; this static
approach keeps the demo simple.
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

# KMS key aliases used in PmeConfig column groups
PCI_KEY_ALIAS = "pwe-hackathon-pci-key"
PII_KEY_ALIAS = "pwe-hackathon-pii-key"

# Static mapping: role/user name substring → set of denied key aliases
# Ordered from most-permissive to most-restrictive.
_ROLE_DENIED_KEYS: list[tuple[str, frozenset[str]]] = [
    # Admin / root / service user — full access
    (":root", frozenset()),
    ("hackathon-service", frozenset()),
    ("AdministratorAccess", frozenset()),
    # Analyst roles
    ("pwe-hackathon-fraud-analyst", frozenset()),
    ("pwe-hackathon-marketing-analyst", frozenset({PCI_KEY_ALIAS})),
    ("pwe-hackathon-junior-analyst", frozenset({PCI_KEY_ALIAS, PII_KEY_ALIAS})),
]

# Default for unknown callers: maximum restriction
_DEFAULT_DENIED_KEYS = frozenset({PCI_KEY_ALIAS, PII_KEY_ALIAS})


def denied_keys_for_caller(caller_arn: str) -> frozenset[str]:
    """Return the set of KMS key aliases to deny for the given caller ARN.

    Matches role names by substring against the caller's IAM ARN. If no
    known role matches, returns the maximum-restriction set (PCI + PII
    denied).

    Parameters
    ----------
    caller_arn : str
        The IAM ARN of the caller (from the Athena federation request's
        ``identity.arn`` field). Can be a role ARN, assumed-role ARN,
        or user ARN.

    Returns
    -------
    frozenset of str
        KMS key aliases whose encrypted columns should be returned as null.

    Examples
    --------
    >>> denied_keys_for_caller("arn:aws:iam::123456:role/pwe-hackathon-fraud-analyst")
    frozenset()
    >>> denied_keys_for_caller("arn:aws:iam::123456:role/pwe-hackathon-junior-analyst")
    frozenset({'pwe-hackathon-pci-key', 'pwe-hackathon-pii-key'})
    """
    if not caller_arn:
        logger.warning("Empty caller ARN — applying maximum restriction")
        return _DEFAULT_DENIED_KEYS

    for role_substring, denied in _ROLE_DENIED_KEYS:
        if role_substring in caller_arn:
            logger.info(
                "RBAC match: caller=%s role=%s denied_keys=%s",
                caller_arn,
                role_substring,
                denied or "(none)",
            )
            return denied

    logger.warning(
        "RBAC: unknown caller %s — applying maximum restriction", caller_arn,
    )
    return _DEFAULT_DENIED_KEYS
