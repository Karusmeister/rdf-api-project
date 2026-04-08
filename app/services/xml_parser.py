"""
XML parser for Polish GAAP financial statements.

Supports multiple XML schema types: SFJINZ (JednostkaInna), SFJMAZ
(JednostkaMala), SFJMIZ (JednostkaMikro), SFJOPZ (JednostkaOp), and
SFZURT (ZakladUbezpieczen).  Schema type is auto-detected from the root
element and dispatched to the matching config in the schema_labels registry.

Flow: ZIP bytes → extract XML → strip namespaces → detect schema → parse tree → structured dict
"""

import io
import re
import time
import zipfile
import xml.etree.ElementTree as ET
from typing import Any, Optional

from app.services.schema_labels import SCHEMA_REGISTRY, SchemaConfig, SectionConfig, detect_schema

# ---------------------------------------------------------------------------
# Backward-compatible alias: TAG_LABELS points to SFJINZ labels.
# External code (analysis routes, tests) may import this directly.
# ---------------------------------------------------------------------------

TAG_LABELS: dict[str, str] = SCHEMA_REGISTRY["SFJINZ"]["tag_labels"]

# NOTE: The ~260-entry SFJINZ label dict formerly inlined here has been moved
# to app/services/schema_labels/sfjinz.py.  TAG_LABELS above is the
# backward-compatible alias pointing to the canonical dict.

# ---------------------------------------------------------------------------
# Simple in-memory cache
# ---------------------------------------------------------------------------

_cache: dict[str, tuple[float, Any]] = {}
CACHE_TTL = 3600.0
CACHE_MAX_SIZE = 512


def cache_get(key: str) -> Any:
    entry = _cache.get(key)
    if entry is None:
        return None
    if (time.time() - entry[0]) >= CACHE_TTL:
        del _cache[key]
        return None
    return entry[1]


def cache_set(key: str, value: Any) -> None:
    now = time.time()
    # Evict all expired entries first
    expired = [k for k, (ts, _) in _cache.items() if now - ts >= CACHE_TTL]
    for k in expired:
        del _cache[k]
    # If still at capacity, evict the oldest entries
    if len(_cache) >= CACHE_MAX_SIZE:
        oldest = sorted(_cache.keys(), key=lambda k: _cache[k][0])
        for k in oldest[: len(_cache) - CACHE_MAX_SIZE + 1]:
            del _cache[k]
    _cache[key] = (now, value)


# ---------------------------------------------------------------------------
# ZIP / XML helpers
# ---------------------------------------------------------------------------

_STATEMENT_MARKER_PREFIXES = ("Bilans", "RZiS", "RachPrzeplywow", "Przeplyw")


def _is_statement_marker(tag: str) -> bool:
    """Return True if *tag* looks like a financial-statement section element."""
    return any(tag.startswith(p) for p in _STATEMENT_MARKER_PREFIXES)


def extract_xml_from_zip(zip_bytes: bytes) -> str:
    """
    Return the financial statement XML from a ZIP archive.
    Prefers any XML whose parsed tree contains Bilans, RZiS, or RachPrzeplywow
    over other XML files (e.g. digital signatures) that may be in the archive.
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        xml_names = sorted(n for n in zf.namelist() if n.lower().endswith(".xml"))
        if not xml_names:
            raise ValueError("No XML file found in ZIP")

        first_content: Optional[str] = None
        for name in xml_names:
            content = zf.read(name).decode("utf-8", errors="replace")
            if first_content is None:
                first_content = content
            try:
                root = ET.fromstring(content)
                # Strip namespaces from tag names for the marker check
                def _local(tag: str) -> str:
                    return tag.split("}", 1)[1] if "}" in tag else tag
                if any(
                    _is_statement_marker(_local(el.tag))
                    for el in root.iter()
                ):
                    return content
            except ET.ParseError:
                continue

        raise ValueError(f"No parseable financial statement XML found in ZIP (files: {xml_names})")


def parse_xml_no_ns(xml_string: str) -> ET.Element:
    """
    Parse XML and strip all namespace URIs from tag and attribute names.
    More robust than text-level regex stripping because the parser handles
    namespace resolution; we then just remove the {uri} prefixes.
    """
    root = ET.fromstring(xml_string)
    for elem in root.iter():
        if "}" in elem.tag:
            elem.tag = elem.tag.split("}", 1)[1]
        new_attrib = {}
        for k, v in elem.attrib.items():
            if "}" in k:
                k = k.split("}", 1)[1]
            new_attrib[k] = v
        elem.attrib = new_attrib
    return root


# ---------------------------------------------------------------------------
# Tree extraction
# ---------------------------------------------------------------------------

def _get_label(tag: str, raw_tag: str, labels: dict[str, str] | None = None) -> str:
    d = labels if labels is not None else TAG_LABELS
    return d.get(tag) or d.get(raw_tag) or tag


def _is_w_tym(tag: str, raw_tag: str, labels: dict[str, str] | None = None) -> bool:
    label = _get_label(tag, raw_tag, labels)
    return label.startswith("(")


def _parse_float(element: ET.Element, child_tag: str) -> float:
    el = element.find(child_tag)
    if el is not None and el.text:
        try:
            return float(el.text.strip().replace(",", "."))
        except ValueError:
            return 0.0
    return 0.0


def extract_tree(
    element: ET.Element,
    depth: int = 0,
    tag_prefix: str = "",
    labels: dict[str, str] | None = None,
    multiplier: int = 1,
) -> Optional[dict]:
    """
    Recursively build a FinancialNode dict from an XML element.
    Only nodes with a KwotaA child are included.

    tag_prefix: "RZiS." for income statement nodes, "CF." for cash flow nodes.
    labels:     Schema-specific tag→label mapping.  Falls back to TAG_LABELS.
    multiplier: Factor applied to all kwota values (1000 for WTys schemas).
    """
    raw_tag = element.tag
    tag = f"{tag_prefix}{raw_tag}" if tag_prefix else raw_tag

    has_direct_kwota = element.find("KwotaA") is not None

    # Recurse into children first — CF container sections (A, B, C) have no
    # direct KwotaA but carry sub-elements (A_I, A_II, A_III) that do.
    children = []
    for child in element:
        if child.tag in ("KwotaA", "KwotaB", "KwotaB1"):
            continue
        child_node = extract_tree(child, depth + 1, tag_prefix, labels, multiplier)
        if child_node is not None:
            children.append(child_node)

    if not has_direct_kwota and not children:
        return None

    if has_direct_kwota:
        kwota_a = _parse_float(element, "KwotaA") * multiplier
        kwota_b = _parse_float(element, "KwotaB") * multiplier
    else:
        # Container node (e.g. CF.A): derive totals from the subtotal child
        # (conventionally the last child, e.g. A_III, B_III, C_III).
        subtotal = children[-1] if children else None
        kwota_a = subtotal["kwota_a"] if subtotal else 0.0
        kwota_b = subtotal["kwota_b"] if subtotal else 0.0

    kwota_b1_el = element.find("KwotaB1")
    kwota_b1: Optional[float] = None
    if kwota_b1_el is not None and kwota_b1_el.text:
        try:
            kwota_b1 = float(kwota_b1_el.text.strip().replace(",", ".")) * multiplier
        except ValueError:
            pass

    return {
        "tag": tag,
        "label": _get_label(tag, raw_tag, labels),
        "kwota_a": kwota_a,
        "kwota_b": kwota_b,
        "kwota_b1": kwota_b1,
        "depth": depth,
        "is_w_tym": _is_w_tym(tag, raw_tag, labels),
        "children": children,
    }


# ---------------------------------------------------------------------------
# Company metadata extraction
# ---------------------------------------------------------------------------

def _extract_company_info(root: ET.Element) -> dict:
    def text(*paths: str) -> Optional[str]:
        for path in paths:
            el = root.find(path)
            if el is not None and el.text:
                return el.text.strip()
        return None

    return {
        "name": text(".//NazwaFirmy"),
        "krs": text(".//P_1E", ".//P_1C"),
        "nip": text(".//P_1D", ".//P_1B"),
        "pkd": text(".//KodPKD"),
        "period_start": text(".//P_3/DataOd", ".//P_2A/DataOd", ".//OkresOd"),
        "period_end": text(".//P_3/DataDo", ".//P_2A/DataDo", ".//OkresDo"),
        "date_prepared": text(".//DataSporzadzenia"),
    }


# ---------------------------------------------------------------------------
# Full statement parsing
# ---------------------------------------------------------------------------

def _extract_rzis(
    root: ET.Element,
    sections: SectionConfig,
    labels: dict[str, str],
    multiplier: int,
) -> tuple[list[dict], Optional[str]]:
    """Extract RZiS nodes; return (nodes, variant_name)."""
    rzis_el = root.find(f".//{sections['rzis_element']}")
    if rzis_el is None:
        return [], None

    rzis_variant: Optional[str] = None
    rzis_content: Optional[ET.Element] = None

    if sections["rzis_has_variants"]:
        rzis_por = rzis_el.find("RZiSPor")
        # Some statements use the canonical "RZiSKalk" element, while older or
        # non-conforming files may still emit "RZiSKal".
        rzis_kalk = rzis_el.find("RZiSKalk")
        rzis_kal = rzis_el.find("RZiSKal")
        rzis_content = (
            rzis_por
            if rzis_por is not None
            else rzis_kalk
            if rzis_kalk is not None
            else rzis_kal
        )
        rzis_variant = (
            "porownawczy" if rzis_por is not None
            else "kalkulacyjny" if (rzis_kalk is not None or rzis_kal is not None)
            else None
        )
    else:
        # Schemas without variant wrapper — children are directly on the element
        rzis_content = rzis_el
        rzis_variant = None

    nodes: list[dict] = []
    if rzis_content is not None:
        for child in rzis_content:
            node = extract_tree(child, depth=0, tag_prefix="RZiS.", labels=labels, multiplier=multiplier)
            if node is not None:
                nodes.append(node)
    return nodes, rzis_variant


def _extract_cf(
    root: ET.Element,
    sections: SectionConfig,
    labels: dict[str, str],
    multiplier: int,
) -> tuple[list[dict], Optional[str]]:
    """Extract cash-flow nodes; return (nodes, method_name)."""
    cf_element_name = sections.get("cf_element")
    if not cf_element_name:
        return [], None

    rach_el = root.find(f".//{cf_element_name}")
    if rach_el is None:
        return [], None

    cf_method: Optional[str] = None
    cf_content: Optional[ET.Element] = None

    if sections["cf_has_variants"]:
        cf_posr = rach_el.find("PrzeplywyPosr")
        cf_bezp = rach_el.find("PrzeplywyBezp")
        cf_content = cf_posr if cf_posr is not None else cf_bezp
        cf_method = (
            "posrednia" if cf_posr is not None
            else "bezposrednia" if cf_bezp is not None
            else None
        )
    else:
        cf_content = rach_el
        cf_method = None

    nodes: list[dict] = []
    if cf_content is not None:
        for child in cf_content:
            node = extract_tree(child, depth=0, tag_prefix="CF.", labels=labels, multiplier=multiplier)
            if node is not None:
                nodes.append(node)
    return nodes, cf_method


_KOD_TO_SCHEMA_CODE: dict[str, str] = {
    "SFJINZ": "SFJINZ",
    "SFJINT": "SFJINZ",  # 2026 thousands variant
    "SFJMAZ": "SFJMAZ",
    "SFJMAT": "SFJMAZ",
    "SFJMIZ": "SFJMIZ",
    "SFJMIT": "SFJMIZ",  # 2026 thousands variant
    "SFJOPZ": "SFJOPZ",
    "SFJOPT": "SFJOPZ",  # 2026 thousands variant
    "SFZURT": "SFZURT",
    "SFZURZ": "SFZURT",
}


def _extract_kod_systemowy(root: ET.Element) -> Optional[str]:
    """Extract normalized `kodSystemowy` (e.g., SFJINZ) from XML header."""
    kod_el = root.find(".//KodSprawozdania")
    if kod_el is None:
        return None

    raw = (kod_el.attrib.get("kodSystemowy") or kod_el.text or "").strip().upper()
    if not raw:
        return None

    # Handles forms like "SFJINZ (1)" and "SFJINZ(2)".
    match = re.match(r"([A-Z0-9]+)", raw)
    if not match:
        return None
    return match.group(1)


def _detect_schema_with_header(root: ET.Element) -> tuple[SchemaConfig, Optional[str]]:
    """
    Detect schema preferring Naglowek/KodSprawozdania@kodSystemowy, then root tag.
    Falls back to SFJINZ for backward compatibility.
    """
    kod_systemowy = _extract_kod_systemowy(root)
    if kod_systemowy:
        mapped_code = _KOD_TO_SCHEMA_CODE.get(kod_systemowy)
        if mapped_code and mapped_code in SCHEMA_REGISTRY:
            return SCHEMA_REGISTRY[mapped_code], kod_systemowy

    schema = detect_schema(root.tag) or SCHEMA_REGISTRY["SFJINZ"]
    return schema, kod_systemowy


def _resolve_unit_multiplier(
    root_tag: str,
    schema: SchemaConfig,
    kod_systemowy: Optional[str],
) -> int:
    """
    Resolve amount multiplier using schema defaults plus currency hints.
    - `...T` kodSystemowy or `...WTys...` root => amounts are in thousands.
    - `...Z` kodSystemowy or `...WZlot...` root => amounts are in PLN.
    """
    if kod_systemowy:
        if kod_systemowy.endswith("T"):
            return 1000
        if kod_systemowy.endswith("Z"):
            return 1

    upper_tag = root_tag.upper()
    if "WTYS" in upper_tag:
        return 1000
    if "WZLOT" in upper_tag:
        return 1
    return schema["sections"]["unit_multiplier"]


def parse_statement(xml_string: str) -> dict:
    """Parse a financial statement XML string into a structured dict.

    Auto-detects the schema type (SFJINZ, SFJMAZ, SFJMIZ, SFJOPZ, SFZURT)
    from the root element and dispatches to the matching config.
    """
    root = parse_xml_no_ns(xml_string)

    schema, kod_systemowy = _detect_schema_with_header(root)
    sections = schema["sections"]
    labels = schema["tag_labels"]
    multiplier = _resolve_unit_multiplier(root.tag, schema, kod_systemowy)

    company = _extract_company_info(root)
    company["schema_type"] = root.tag
    company["schema_code"] = schema["code"]
    company["kod_systemowy"] = kod_systemowy

    # Bilans
    aktywa_node: Optional[dict] = None
    pasywa_node: Optional[dict] = None
    bilans_el = root.find(f".//{sections['bilans_element']}")
    if bilans_el is not None:
        a_el = bilans_el.find(sections["aktywa_element"])
        p_el = bilans_el.find(sections["pasywa_element"])
        if a_el is not None:
            aktywa_node = extract_tree(a_el, depth=0, labels=labels, multiplier=multiplier)
        if p_el is not None:
            pasywa_node = extract_tree(p_el, depth=0, labels=labels, multiplier=multiplier)

    # RZiS
    rzis_nodes, rzis_variant = _extract_rzis(root, sections, labels, multiplier)

    # Cash flow
    cf_nodes, cf_method = _extract_cf(root, sections, labels, multiplier)

    # Extra sections (equity changes, off-balance-sheet, etc.)
    extras: dict[str, list[dict]] = {}
    for extra_def in sections.get("extra_sections", []):
        el = root.find(f".//{extra_def['element']}")
        if el is not None:
            nodes: list[dict] = []
            for child in el:
                node = extract_tree(child, depth=0, tag_prefix=extra_def["tag_prefix"], labels=labels, multiplier=multiplier)
                if node is not None:
                    nodes.append(node)
            if nodes:
                extras[extra_def["store_as"]] = nodes

    company["rzis_variant"] = rzis_variant
    company["cf_method"] = cf_method

    return {
        "company": company,
        "bilans": {"aktywa": aktywa_node, "pasywa": pasywa_node},
        "rzis": rzis_nodes,
        "cash_flow": cf_nodes,
        "extras": extras,
    }


# ---------------------------------------------------------------------------
# Tree lookup helpers
# ---------------------------------------------------------------------------

def find_node_value(
    tree: Any,
    tag: str,
    kwota: str = "kwota_a",
) -> Optional[float]:
    """Recursively find a node by tag and return its kwota value."""
    if tree is None:
        return None
    if isinstance(tree, dict):
        if tree.get("tag") == tag:
            return tree.get(kwota, 0.0)
        for child in tree.get("children", []):
            result = find_node_value(child, tag, kwota)
            if result is not None:
                return result
    elif isinstance(tree, list):
        for item in tree:
            result = find_node_value(item, tag, kwota)
            if result is not None:
                return result
    return None


def find_value(stmt: dict, tag: str, kwota: str = "kwota_a") -> Optional[float]:
    """Search across bilans + rzis + cash_flow + extras."""
    bilans = stmt.get("bilans") or {}
    for section in (bilans.get("aktywa"), bilans.get("pasywa")):
        if section is None:
            continue
        v = find_node_value(section, tag, kwota)
        if v is not None:
            return v
    rzis = stmt.get("rzis")
    if rzis is not None:
        v = find_node_value(rzis, tag, kwota)
        if v is not None:
            return v
    cash_flow = stmt.get("cash_flow")
    if cash_flow is not None:
        v = find_node_value(cash_flow, tag, kwota)
        if v is not None:
            return v
    for section_nodes in stmt.get("extras", {}).values():
        v = find_node_value(section_nodes, tag, kwota)
        if v is not None:
            return v
    return None


def extract_flat_values(stmt: dict, use_kwota_b: bool = False) -> dict[str, Optional[float]]:
    """
    Flatten all nodes in a parsed statement to {tag: value}.
    use_kwota_b=True extracts the embedded prior-year column.
    """
    kwota_key = "kwota_b" if use_kwota_b else "kwota_a"
    result: dict[str, Optional[float]] = {}

    def _traverse(node: Any) -> None:
        if isinstance(node, dict):
            if "tag" in node:
                result[node["tag"]] = node.get(kwota_key)
            for child in node.get("children", []):
                _traverse(child)
        elif isinstance(node, list):
            for item in node:
                _traverse(item)

    _traverse(stmt["bilans"]["aktywa"])
    _traverse(stmt["bilans"]["pasywa"])
    _traverse(stmt["rzis"])
    _traverse(stmt["cash_flow"])
    for section_nodes in stmt.get("extras", {}).values():
        _traverse(section_nodes)
    return result


# ---------------------------------------------------------------------------
# Comparison tree building
# ---------------------------------------------------------------------------

def _safe_pct(current: float, previous: float) -> Optional[float]:
    if previous == 0:
        return None
    return round((current - previous) / abs(previous) * 100, 2)


def _safe_share(value: float, parent: Optional[float]) -> Optional[float]:
    if parent is None or parent == 0:
        return None
    return round(value / parent * 100, 4)


def node_to_comparison(
    node: dict,
    parent_current: Optional[float] = None,
    parent_previous: Optional[float] = None,
    previous_node: Optional[dict] = None,
    use_kwota_b_fallback: bool = True,
) -> dict:
    """
    Convert a FinancialNode to a ComparisonNode.
    If previous_node is provided, uses its kwota_a as the previous value.
    If use_kwota_b_fallback is True and previous_node is absent, falls back to
    node.kwota_b (embedded prior-year column) — correct only for single-statement mode.
    If use_kwota_b_fallback is False and previous_node is absent, previous is None
    and all derived change/share fields are also None — correct for two-statement mode.
    """
    current = node.get("kwota_a", 0.0)
    if previous_node is not None:
        previous: Optional[float] = previous_node.get("kwota_a", 0.0)
    elif use_kwota_b_fallback:
        previous = node.get("kwota_b", 0.0)
    else:
        previous = None

    prev_children_map: dict[str, dict] = {}
    if previous_node:
        for c in previous_node.get("children", []):
            prev_children_map[c["tag"]] = c

    children = []
    for child in node.get("children", []):
        prev_child = prev_children_map.get(child["tag"]) if previous_node else None
        children.append(
            node_to_comparison(child, current, previous, prev_child, use_kwota_b_fallback)
        )

    if previous is not None:
        change_absolute: Optional[float] = round(current - previous, 2)
        change_percent = _safe_pct(current, previous)
        share_prev = _safe_share(previous, parent_previous)
    else:
        change_absolute = None
        change_percent = None
        share_prev = None

    return {
        "tag": node["tag"],
        "label": node["label"],
        "current": current,
        "previous": previous,
        "change_absolute": change_absolute,
        "change_percent": change_percent,
        "share_of_parent_current": _safe_share(current, parent_current),
        "share_of_parent_previous": share_prev,
        "depth": node["depth"],
        "is_w_tym": node["is_w_tym"],
        "children": children,
    }


def _is_empty_comparison_tree(node: Optional[dict]) -> bool:
    """Detect a synthetic or extracted comparison root that carries no data.

    A statement tree is "empty" when the top-level node has `current` and
    `previous` both null AND every descendant also has `current` and
    `previous` null (including the degenerate case of no descendants at
    all). Returning True here is the trigger for `build_comparison` to emit
    `None` for that field instead of a stub root the frontend would render
    as an empty table (see backend_changes.json: jednostkainna-empty-statement-trees).

    Bilans aktywa/pasywa are also statement trees and use the same rule, so
    this helper is shared across rzis, cash_flow, and the balance-sheet
    halves. The preferred long-term fix — mapping JednostkaInna-specific tag
    paths into the common RZiS.*/CF.* taxonomy — still benefits: once the
    mapping is in place, populated trees stop triggering this branch
    automatically.
    """
    if node is None:
        return True
    if node.get("current") is not None or node.get("previous") is not None:
        return False
    for child in node.get("children", []) or []:
        if not _is_empty_comparison_tree(child):
            return False
    return True


# Synthetic root labels for wrapping the top-level RZiS / CF node list into
# a single ComparisonNode. The frontend expects a single tree per statement
# section (mirroring `bilans.aktywa` / `bilans.pasywa`), not a bare list.
_SYNTHETIC_ROOTS: dict[str, tuple[str, str]] = {
    "rzis": ("RZiS", "Rachunek zysków i strat"),
    "cash_flow": ("CF", "Rachunek przepływów pieniężnych"),
}


def _wrap_section_as_root(section: str, children: list[dict]) -> dict:
    """Turn the list of top-level RZiS/CF comparison nodes into a single
    synthetic root that matches the `ComparisonNode` contract.

    The synthetic root carries no numeric totals because RZiS / CF don't have
    a single "grand total" line (unlike Aktywa / Pasywa which balance to a
    single sum). `current` and `previous` are therefore explicitly None, and
    all derived fields are None too.
    """
    tag, label = _SYNTHETIC_ROOTS[section]
    return {
        "tag": tag,
        "label": label,
        "current": None,
        "previous": None,
        "change_absolute": None,
        "change_percent": None,
        "share_of_parent_current": None,
        "share_of_parent_previous": None,
        "depth": 0,
        "is_w_tym": False,
        "children": children,
    }


def build_comparison(
    current_stmt: dict,
    previous_stmt: Optional[dict] = None,
) -> dict:
    """
    Build a full comparison structure from one or two parsed statements.
    If previous_stmt is None, uses kwota_b (embedded column) from current_stmt.
    If previous_stmt is provided, only that statement's kwota_a is used as the
    previous value — kwota_b from current_stmt is never read.

    Contract:
      * bilans.aktywa / bilans.pasywa: `ComparisonNode | None`
      * rzis / cash_flow: `ComparisonNode | None` — a single root whose
        `children` holds the top-level statement items, or `None` when the
        underlying statement section is empty or carries no real values.

    Before the jednostkainna-empty-statement-trees fix, rzis and cash_flow
    were returned as bare lists. That mismatched the frontend contract
    (`ComparisonNode | null`) and caused empty sections to render as a
    single-row stub instead of the intended "unavailable" message.
    """
    # Use kwota_b fallback only in single-statement mode
    use_kwota_b = previous_stmt is None

    def _find_node(stmt: dict, tag: str) -> Optional[dict]:
        for section in (stmt["bilans"]["aktywa"], stmt["bilans"]["pasywa"]):
            result = _find_in_tree(section, tag)
            if result:
                return result
        for item in stmt["rzis"]:
            result = _find_in_tree(item, tag)
            if result:
                return result
        for item in stmt["cash_flow"]:
            result = _find_in_tree(item, tag)
            if result:
                return result
        return None

    def _find_in_tree(node: Optional[dict], tag: str) -> Optional[dict]:
        if node is None:
            return None
        if node.get("tag") == tag:
            return node
        for child in node.get("children", []):
            result = _find_in_tree(child, tag)
            if result:
                return result
        return None

    def cmp_tree(node: Optional[dict]) -> Optional[dict]:
        if node is None:
            return None
        prev_node = None
        if previous_stmt is not None:
            prev_node = _find_node(previous_stmt, node["tag"])
        return node_to_comparison(node, None, None, prev_node, use_kwota_b)

    def cmp_list(nodes: list[dict]) -> list[dict]:
        result = []
        for node in nodes:
            prev_node = None
            if previous_stmt is not None:
                prev_node = _find_node(previous_stmt, node["tag"])
            result.append(node_to_comparison(node, None, None, prev_node, use_kwota_b))
        return result

    def _build_section(section_key: str) -> Optional[dict]:
        """Assemble rzis/cash_flow as a single synthetic root or `None`."""
        items = cmp_list(current_stmt[section_key])
        root = _wrap_section_as_root(section_key, items)
        if _is_empty_comparison_tree(root):
            return None
        return root

    return {
        "bilans": {
            "aktywa": cmp_tree(current_stmt["bilans"]["aktywa"]),
            "pasywa": cmp_tree(current_stmt["bilans"]["pasywa"]),
        },
        "rzis": _build_section("rzis"),
        "cash_flow": _build_section("cash_flow"),
    }


# ---------------------------------------------------------------------------
# Semantic tag aliases — maps schema-neutral concepts to schema-specific tags
# ---------------------------------------------------------------------------

_SEMANTIC_TAGS: dict[str, dict[str, str]] = {
    "total_assets":            {"SFJINZ": "Aktywa", "SFJMAZ": "Aktywa", "SFJMIZ": "Aktywa", "SFJOPZ": "Aktywa", "SFZURT": "Aktywa"},
    "equity":                  {"SFJINZ": "Pasywa_A", "SFJMAZ": "Pasywa_A", "SFJMIZ": "Pasywa_A", "SFJOPZ": "Pasywa_A", "SFZURT": "Pasywa_A"},
    "current_assets":          {"SFJINZ": "Aktywa_B", "SFJMAZ": "Aktywa_B", "SFJMIZ": "Aktywa_B", "SFJOPZ": "Aktywa_B"},
    "total_liabilities":       {"SFJINZ": "Pasywa_B", "SFJMAZ": "Pasywa_B", "SFJMIZ": "Pasywa_B", "SFJOPZ": "Pasywa_B"},
    "short_term_liabilities":  {"SFJINZ": "Pasywa_B_III", "SFJMAZ": "Pasywa_B_III", "SFJOPZ": "Pasywa_B_III"},
    "revenue":                 {"SFJINZ": "RZiS.A", "SFJMAZ": "RZiS.A", "SFJMIZ": "RZiS.A", "SFJOPZ": "RZiS.A", "SFZURT": "RZiS.I"},
    "operating_profit":        {"SFJINZ": "RZiS.F", "SFJMAZ": "RZiS.C", "SFJOPZ": "RZiS.H", "SFZURT": "RZiS.XI"},
    # SFZURT: XIV = "Zysk (strata) brutto" (gross profit, before tax).
    # The Annex 3 RZiS has no single net-profit summary line (net = XIV - XV - XVI);
    # omitting SFZURT here so net_profit resolves to None and net_margin is null.
    "gross_profit":            {"SFZURT": "RZiS.XIV"},
    "net_profit":              {"SFJINZ": "RZiS.L", "SFJMAZ": "RZiS.J", "SFJMIZ": "RZiS.F", "SFJOPZ": "RZiS.O"},
}


def resolve_tag(concept: str, schema_code: str) -> Optional[str]:
    """Resolve a semantic concept to the tag_path for the given schema.

    Public API — used by both ``compute_ratios`` and analysis route endpoints.
    """
    mapping = _SEMANTIC_TAGS.get(concept)
    if mapping is None:
        return None
    return mapping.get(schema_code)


# ---------------------------------------------------------------------------
# Financial ratios
# ---------------------------------------------------------------------------

def compute_ratios(stmt: dict, use_kwota_b: bool = False) -> dict:
    """Compute key financial ratios from a parsed statement.

    Schema-aware: resolves tag paths using the statement's schema_code so
    that ratios work correctly for all five supported schemas.
    """
    kwota = "kwota_b" if use_kwota_b else "kwota_a"
    schema_code = stmt.get("company", {}).get("schema_code", "SFJINZ")

    def v(concept: str) -> Optional[float]:
        tag = resolve_tag(concept, schema_code)
        if tag is None:
            return None
        return find_value(stmt, tag, kwota)

    def ratio(num: Optional[float], den: Optional[float]) -> Optional[float]:
        if num is None or den is None or abs(den) < 1e-6:
            return None
        return round(num / den, 4)

    aktywa = v("total_assets")
    equity = v("equity")
    current_assets = v("current_assets")
    total_liab = v("total_liabilities")
    short_term_liab = v("short_term_liabilities")
    revenue = v("revenue")
    op_profit = v("operating_profit")
    net_profit = v("net_profit")

    return {
        "equity_ratio": ratio(equity, aktywa),
        "current_ratio": ratio(current_assets, short_term_liab),
        "debt_ratio": ratio(total_liab, aktywa),
        "operating_margin": ratio(op_profit, revenue),
        "net_margin": ratio(net_profit, revenue),
    }
