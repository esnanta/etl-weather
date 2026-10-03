"""Compatibility imports for configuration moved to project-level owners.

New code should import lightweight values from :mod:`config` and request
construction from :mod:`request_factory`.
"""

from config import (
    CDS_DATASET,
    CDS_END_DATE,
    CDS_START_DATE,
    CDS_VARIABLES,
)
from request_factory import build_aceh_bbox, build_cds_request

# Backward-compatible typed bounding box for callers not yet migrated.
ACEH_BBOX = build_aceh_bbox()

__all__ = [
    "ACEH_BBOX",
    "CDS_DATASET",
    "CDS_END_DATE",
    "CDS_START_DATE",
    "CDS_VARIABLES",
    "build_cds_request",
]
