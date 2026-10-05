"""Compatibility imports for configuration moved to project-level owners.

New code should import lightweight values from :mod:`config` and request
construction from :mod:`pipeline.requests.factory`.
"""

from config import CDS_DATASET, CDS_VARIABLES, resolve_cds_period
from pipeline.requests.factory import build_aceh_bbox, build_cds_request

ACEH_BBOX = build_aceh_bbox()


def configured_cds_period() -> tuple[str, str]:
    """Return the standalone CDS period configured through the environment."""
    return resolve_cds_period()


__all__ = [
    "ACEH_BBOX",
    "CDS_DATASET",
    "CDS_VARIABLES",
    "build_cds_request",
    "configured_cds_period",
]
