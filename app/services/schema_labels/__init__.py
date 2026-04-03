"""
Schema-config registry for Polish e-Sprawozdania XML financial statements.

Each supported XML schema (SFJINZ, SFJMAZ, SFJMIZ, SFJOPZ, SFZURT) is
registered as a SchemaConfig dict containing element names, tag labels, and
section configuration.  The parser detects schema type from the root element
name after namespace stripping and dispatches to the matching config.
"""

from typing import Optional, TypedDict


class ExtraSectionDef(TypedDict):
    element: str    # XML element name to search for (e.g. "ZestZmianWKapitale")
    store_as: str   # Section name for raw_financial_data (e.g. "equity_changes")
    tag_prefix: str # Prefix for flattened tag_paths (e.g. "EQ.")


class SectionConfig(TypedDict):
    bilans_element: str       # e.g. "Bilans", "BilansJednostkaMala"
    aktywa_element: str       # e.g. "Aktywa"
    pasywa_element: str       # e.g. "Pasywa"
    rzis_element: str         # e.g. "RZiS", "RZiSJednostkaMala"
    rzis_has_variants: bool   # True → look for RZiSPor/RZiSKalk wrapper
    cf_element: Optional[str] # e.g. "RachPrzeplywow" or None if no CF
    cf_has_variants: bool     # True → look for PrzeplywyPosr/PrzeplywyBezp
    unit_multiplier: int      # 1 for PLN, 1000 for "WTys" schemas
    extra_sections: list[ExtraSectionDef]  # Additional sections to parse


class SchemaConfig(TypedDict):
    code: str                          # "SFJINZ", "SFJMAZ", etc.
    root_tags: list[str]               # Root element names after ns stripping
    sections: SectionConfig
    tag_labels: dict[str, str]         # tag → Polish label
    statement_markers: list[str]       # Element prefixes to detect in ZIP/dir


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

SCHEMA_REGISTRY: dict[str, SchemaConfig] = {}

_ROOT_TAG_INDEX: dict[str, SchemaConfig] = {}


def register_schema(config: SchemaConfig) -> None:
    """Register a schema config and index its root tags for fast lookup."""
    SCHEMA_REGISTRY[config["code"]] = config
    for tag in config["root_tags"]:
        _ROOT_TAG_INDEX[tag] = config


def detect_schema(root_tag: str) -> Optional[SchemaConfig]:
    """Map a namespace-stripped root tag to its SchemaConfig."""
    return _ROOT_TAG_INDEX.get(root_tag)


# Auto-register all schemas on import
from app.services.schema_labels import sfjinz as _sfjinz  # noqa: E402,F401
from app.services.schema_labels import sfjmaz as _sfjmaz  # noqa: E402,F401
from app.services.schema_labels import sfjmiz as _sfjmiz  # noqa: E402,F401
from app.services.schema_labels import sfjopz as _sfjopz  # noqa: E402,F401
from app.services.schema_labels import sfzurt as _sfzurt  # noqa: E402,F401
