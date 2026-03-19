# RDF API Extension - Financial Statement Comparison

## Context

You are extending an existing FastAPI proxy service that wraps the Polish KRS (National Court Register) financial document API. The existing codebase already handles KRS lookup, document search, metadata retrieval, and ZIP download of raw XML financial statements.

Your task: add new endpoints that **parse** the downloaded XML financial statements and expose structured, comparison-ready JSON data.

Read the existing API documentation in `API.md` for full context on existing endpoints. This document covers only the NEW functionality to build.

---

## Architecture Decision

Add a new router module (e.g. `app/routers/analysis.py`) with these endpoints. The parsing logic should live in a separate service module (e.g. `app/services/xml_parser.py`).

The flow for each new endpoint:
1. Accept KRS + optional filters from the client
2. Internally call the existing `/api/dokumenty/search` logic to find financial statements (`rodzaj == "18"`)
3. Internally call the existing `/api/dokumenty/download` logic to get the ZIP(s)
4. Extract XML from ZIP, parse it, return structured JSON

Do NOT require the client to download ZIPs and re-upload them. The API handles everything server-side.

---

## New Endpoints

---

### POST /api/analysis/statement

Parse a single financial statement and return its full hierarchical structure.

**Request body**
```json
{
  "krs": "694720",
  "period_end": "2024-12-31"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `krs` | string | yes | KRS number (leading zeros optional) |
| `period_end` | string | no | Reporting period end date `YYYY-MM-DD`. If omitted, returns the most recent statement. |

**Response `200`**
```json
{
  "company": {
    "name": "B-JWK-MANAGEMENT SPOLKA Z OGRANICZONA ODPOWIEDZIALNOSCIA",
    "krs": "0000694720",
    "nip": "5842734981",
    "pkd": "6810Z",
    "period_start": "2024-01-01",
    "period_end": "2024-12-31",
    "date_prepared": "2025-04-28",
    "schema_type": "JednostkaInna",
    "rzis_variant": "porownawczy"
  },
  "bilans": {
    "aktywa": {
      "tag": "Aktywa",
      "label": "AKTYWA",
      "kwota_a": 115365276.82,
      "kwota_b": 162710400.36,
      "depth": 0,
      "is_w_tym": false,
      "children": [
        {
          "tag": "Aktywa_A",
          "label": "A. Aktywa trwale",
          "kwota_a": 68788128.60,
          "kwota_b": 71878620.14,
          "depth": 1,
          "is_w_tym": false,
          "children": [
            {
              "tag": "Aktywa_A_I",
              "label": "I. Wartosci niematerialne i prawne",
              "kwota_a": 2027.71,
              "kwota_b": 4006.92,
              "depth": 2,
              "is_w_tym": false,
              "children": [
                {
                  "tag": "Aktywa_A_I_1",
                  "label": "1. Koszty zakonczonych prac rozwojowych",
                  "kwota_a": 0.00,
                  "kwota_b": 0.00,
                  "depth": 3,
                  "is_w_tym": false,
                  "children": []
                }
              ]
            }
          ]
        }
      ]
    },
    "pasywa": { }
  },
  "rzis": { },
  "cash_flow": { }
}
```

Each node in the tree follows this schema:

```python
class FinancialNode(BaseModel):
    tag: str                        # XML tag suffix, e.g. "Aktywa_A_II_1_B"
    label: str                      # Human-readable Polish label
    kwota_a: float                  # Current reporting year
    kwota_b: float                  # Previous year
    kwota_b1: float | None = None   # Year before previous (if present)
    depth: int                      # Nesting level (0 = root)
    is_w_tym: bool                  # True for "w tym" (of which) fields
    children: list["FinancialNode"]
```

---

### POST /api/analysis/compare

Compare two financial statements year-over-year. Returns the merged tree with change calculations at every level.

**Request body**
```json
{
  "krs": "694720",
  "period_end_current": "2024-12-31",
  "period_end_previous": "2023-12-31"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `krs` | string | yes | KRS number |
| `period_end_current` | string | yes | End date of the "current" year |
| `period_end_previous` | string | yes | End date of the "previous" year |

NOTE: In most cases the XML already contains both years (`kwota_a` = current, `kwota_b` = previous). If `period_end_previous` matches the previous year embedded in the current statement, you can serve the comparison from a single XML. Only download two separate files if the years don't line up (e.g. comparing 2024 vs 2021).

**Response `200`**
```json
{
  "company": { },
  "current_period": { "start": "2024-01-01", "end": "2024-12-31" },
  "previous_period": { "start": "2023-01-01", "end": "2023-12-31" },
  "bilans": {
    "aktywa": {
      "tag": "Aktywa",
      "label": "AKTYWA",
      "current": 115365276.82,
      "previous": 162710400.36,
      "change_absolute": -47345123.54,
      "change_percent": -29.10,
      "share_of_parent_current": 100.0,
      "share_of_parent_previous": 100.0,
      "depth": 0,
      "is_w_tym": false,
      "children": [ ]
    },
    "pasywa": { }
  },
  "rzis": { },
  "cash_flow": { },
  "ratios": {
    "equity_ratio": { "current": 0.747, "previous": 0.422, "change": 0.325 },
    "current_ratio": { "current": 17.15, "previous": 1.071, "change": 16.08 },
    "debt_ratio": { "current": 0.253, "previous": 0.578, "change": -0.325 },
    "operating_margin": { "current": 0.592, "previous": 0.201, "change": 0.391 },
    "net_margin": { "current": 0.487, "previous": 0.144, "change": 0.343 },
    "revenue_change_pct": 13.03,
    "net_profit_change_pct": 281.03
  }
}
```

Each comparison node schema:

```python
class ComparisonNode(BaseModel):
    tag: str
    label: str
    current: float
    previous: float
    change_absolute: float              # current - previous
    change_percent: float | None        # None when previous == 0
    share_of_parent_current: float | None   # this.current / parent.current * 100
    share_of_parent_previous: float | None  # this.previous / parent.previous * 100
    depth: int
    is_w_tym: bool
    children: list["ComparisonNode"]
```

**Ratio calculations:**

| Ratio | Formula | Source fields |
|-------|---------|---------------|
| `equity_ratio` | Kapital wlasny / Aktywa | `Pasywa_A.kwota / Aktywa.kwota` |
| `current_ratio` | Aktywa obrotowe / Zobowiazania krotkoterminowe | `Aktywa_B.kwota / Pasywa_B_III.kwota` |
| `debt_ratio` | Zobowiazania / Aktywa | `Pasywa_B.kwota / Aktywa.kwota` |
| `operating_margin` | Zysk operacyjny / Przychody | `RZiS.F.kwota / RZiS.A.kwota` |
| `net_margin` | Zysk netto / Przychody | `RZiS.L.kwota / RZiS.A.kwota` |
| `revenue_change_pct` | (A_current - A_previous) / abs(A_previous) * 100 | RZiS.A |
| `net_profit_change_pct` | (L_current - L_previous) / abs(L_previous) * 100 | RZiS.L |

Return `null` for any ratio where the denominator is 0.

---

### POST /api/analysis/time-series

Track selected financial fields across multiple years for trend analysis.

**Request body**
```json
{
  "krs": "694720",
  "fields": [
    "Aktywa",
    "Aktywa_A",
    "Aktywa_B",
    "Pasywa_A",
    "Pasywa_B",
    "RZiS.A",
    "RZiS.L",
    "RZiS.F"
  ],
  "period_ends": ["2024-12-31", "2023-12-31", "2022-12-31"]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `krs` | string | yes | KRS number |
| `fields` | array[string] | yes | List of tag names to track. Bilans tags use the `Aktywa_*` / `Pasywa_*` format. RZiS tags are prefixed with `RZiS.` (e.g. `RZiS.A`, `RZiS.L`). Cash flow tags prefixed with `CF.` (e.g. `CF.A_III`). |
| `period_ends` | array[string] | no | Specific period end dates to include. If omitted, return ALL available years (sorted chronologically). |

**Response `200`**
```json
{
  "company": {
    "name": "B-JWK-MANAGEMENT ...",
    "krs": "0000694720"
  },
  "periods": [
    { "start": "2022-01-01", "end": "2022-12-31" },
    { "start": "2023-01-01", "end": "2023-12-31" },
    { "start": "2024-01-01", "end": "2024-12-31" }
  ],
  "series": [
    {
      "tag": "Aktywa",
      "label": "AKTYWA",
      "section": "bilans",
      "values": [null, 162710400.36, 115365276.82],
      "changes_absolute": [null, null, -47345123.54],
      "changes_percent": [null, null, -29.10]
    },
    {
      "tag": "RZiS.A",
      "label": "A. Przychody netto ze sprzedazy i zrownane z nimi",
      "section": "rzis",
      "values": [null, 38309180.85, 43301427.01],
      "changes_absolute": [null, null, 4992246.16],
      "changes_percent": [null, null, 13.03]
    }
  ]
}
```

**How to reconstruct values across years:**

Each XML file contains `kwota_a` (its own year) and `kwota_b` (the year before). To build a time series:

1. Download all financial statements for the KRS (`rodzaj == "18"`, `status == "NIEUSUNIETY"`)
2. For each file, extract `kwota_a` (the file's own year) for all requested fields
3. For the oldest file, also use `kwota_b` to get one extra year of data
4. Merge by period end date, sort chronologically
5. If `period_ends` is specified, filter to only those periods
6. If a field is missing in a particular year's XML (not all years have the same schema), set that value to `null`

Changes are always calculated relative to the previous period in the array (index N vs index N-1). First element's change is always `null`.

---

### GET /api/analysis/available-periods/{krs}

List all available financial statement periods for a company.

**Path parameter:** `krs` - KRS number

**Response `200`**
```json
{
  "krs": "0000694720",
  "company_name": "B-JWK-MANAGEMENT ...",
  "periods": [
    {
      "period_start": "2018-01-01",
      "period_end": "2018-12-31",
      "document_id": "abc123==",
      "date_filed": "2019-07-15",
      "is_correction": false,
      "is_ifrs": false,
      "filename": "Bjwk SF za 2018.xml"
    },
    {
      "period_start": "2024-01-01",
      "period_end": "2024-12-31",
      "document_id": "ZgsX8Fsncb1PFW07-T4XoQ==",
      "date_filed": "2025-05-20",
      "is_correction": false,
      "is_ifrs": false,
      "filename": "Bjwk SF za 2024.xml"
    }
  ]
}
```

This endpoint only calls `/search` + `/metadata` - it does NOT download or parse XML. It gives the frontend the list of years to offer in the period selector.

Filter: only include documents where `rodzaj == "18"` (annual financial statement), `status == "NIEUSUNIETY"`, and `czyMSR == false` (Polish GAAP only - IFRS/XHTML parsing is out of scope). If there are corrections (`czyKorekta == true`), prefer the correction over the original for the same period.

---

## XML Parsing Implementation Guide

### Namespace Handling

The XML uses namespaces that CHANGE between schema versions. Never hardcode namespace URIs. Use this approach:

```python
import xml.etree.ElementTree as ET

def parse_xml(xml_string: str) -> ET.Element:
    """Parse XML and build a namespace map dynamically."""
    root = ET.fromstring(xml_string)
    # Build reverse namespace map from root attributes
    ns = {}
    for attr, uri in root.attrib.items():
        if attr.startswith('{'):
            continue
        if 'xmlns:' in attr or attr == 'xmlns':
            prefix = attr.split(':')[1] if ':' in attr else ''
            ns[prefix] = uri
    return root, ns
```

Better approach - strip namespaces entirely for simpler querying:

```python
import re

def strip_namespaces(xml_string: str) -> str:
    """Remove all namespace prefixes for easier parsing."""
    # Remove namespace declarations
    xml_string = re.sub(r'\s+xmlns(:\w+)?="[^"]*"', '', xml_string)
    # Remove namespace prefixes from tags
    xml_string = re.sub(r'<(/?)[\w]+:', r'<\1', xml_string)
    return xml_string
```

After stripping, you can use simple `element.find('Aktywa_A_II_1_B')`.

### Tree Extraction

Recursively walk the XML tree. A node is a "financial item" if it has a `KwotaA` child element.

```python
def extract_tree(element: ET.Element, depth: int = 0) -> FinancialNode | None:
    """Recursively extract financial data from an XML element."""
    kwota_a_el = element.find('KwotaA')
    if kwota_a_el is None:
        return None

    tag = element.tag  # e.g. "Aktywa_A_II_1_B"
    kwota_a = float(kwota_a_el.text or "0")
    kwota_b = float((element.find('KwotaB') or ET.Element('')).text or "0")
    kwota_b1_el = element.find('KwotaB1')
    kwota_b1 = float(kwota_b1_el.text) if kwota_b1_el is not None and kwota_b1_el.text else None

    children = []
    for child in element:
        if child.tag in ('KwotaA', 'KwotaB', 'KwotaB1'):
            continue
        child_node = extract_tree(child, depth + 1)
        if child_node is not None:
            children.append(child_node)

    # Detect "w tym" (of which) fields
    is_w_tym = tag.endswith('_J') or tag.endswith('_1') and '_J' in tag

    return FinancialNode(
        tag=tag,
        label=TAG_LABELS.get(tag, tag),
        kwota_a=kwota_a,
        kwota_b=kwota_b,
        kwota_b1=kwota_b1,
        depth=depth,
        is_w_tym=is_w_tym,
        children=children,
    )
```

### Detecting "w tym" (of which) fields

These are subset indicators, NOT additive children. They show what portion of the parent comes from a specific source. In the XML they appear as tags ending with `_J` inside RZiS:

| Tag | Meaning |
|-----|---------|
| `A_J` | (w tym: od jednostek powiazanych) - inside RZiS.A |
| `G_II_J` | (w tym: od jednostek powiazanych) - inside RZiS.G.II |
| `G_III_J` | (w tym: w jednostkach powiazanych) - inside RZiS.G.III |
| `H_I_J` | (w tym: dla jednostek powiazanych) - inside RZiS.H.I |
| `H_II_J` | (w tym: w jednostkach powiazanych) - inside RZiS.H.II |
| `B_IV_1` | (w tym: podatek akcyzowy) - inside RZiS.B.IV |
| `B_VI_1` | (w tym: emerytalne) - inside RZiS.B.VI |

Detection rule: within RZiS, a tag ending in `_J` is always "w tym". Tags like `B_IV_1` and `B_VI_1` are also "w tym" but are harder to detect automatically - use the label dictionary to flag them.

### Section Detection

Find each section in the XML:

```python
root = ET.fromstring(stripped_xml)

# Bilans
bilans = root.find('.//Bilans')
aktywa = bilans.find('Aktywa') if bilans is not None else None
pasywa = bilans.find('Pasywa') if bilans is not None else None

# RZiS - check which variant exists
rzis_el = root.find('.//RZiS')
rzis_por = rzis_el.find('RZiSPor') if rzis_el is not None else None  # porownawczy
rzis_kal = rzis_el.find('RZiSKal') if rzis_el is not None else None  # kalkulacyjny
rzis = rzis_por or rzis_kal
rzis_variant = "porownawczy" if rzis_por is not None else "kalkulacyjny" if rzis_kal is not None else None

# Cash flow - check method
rach_przeplywow = root.find('.//RachPrzeplywow')
cf_posr = rach_przeplywow.find('PrzeplywyPosr') if rach_przeplywow is not None else None  # indirect
cf_bezp = rach_przeplywow.find('PrzeplywyBezp') if rach_przeplywow is not None else None  # direct
cash_flow = cf_posr or cf_bezp

# Company metadata
intro = root.find('.//WprowadzenieDoSprawozdaniaFinansowego')
header = root.find('.//Naglowek')
```

### Extracting Company Metadata

```python
def extract_company_info(root: ET.Element) -> dict:
    def text(path: str) -> str | None:
        el = root.find(path)
        return el.text.strip() if el is not None and el.text else None

    intro = root.find('.//WprowadzenieDoSprawozdaniaFinansowego')
    header = root.find('.//Naglowek')

    return {
        "name": text('.//NazwaFirmy'),
        "krs": text('.//P_1E'),
        "nip": text('.//P_1D'),
        "pkd": text('.//KodPKD'),
        "period_start": text('.//P_3/DataOd') or text('.//OkresOd'),
        "period_end": text('.//P_3/DataDo') or text('.//OkresDo'),
        "date_prepared": text('.//DataSporzadzenia'),
        "schema_type": root.tag,  # e.g. "JednostkaInna"
    }
```

### Entity Type Variations

The XML root element indicates the entity type. Each has a slightly different schema (different namespace URIs, sometimes different available sections):

| Root tag | Entity type | Notes |
|----------|-------------|-------|
| `JednostkaInna` | Standard entity | Most complete structure (Zalacznik nr 1) |
| `JednostkaMikro` | Micro entity | Simplified balance sheet, no cash flow |
| `JednostkaMala` | Small entity | Simplified structure |
| `JednostkaOPP` | Public benefit org | Similar to JednostkaInna |

The parser should work generically - the recursive tree extraction handles all types. The tag-to-label mapping just needs entries for each type's tags. Start with `JednostkaInna` (most common for sp. z o.o. companies) and extend labels as needed.

---

## Tag-to-Label Dictionary

Below is the COMPLETE mapping for the `JednostkaInna` schema. Store this as a Python dict or JSON file.

```python
TAG_LABELS = {
    # === BILANS - AKTYWA ===
    "Aktywa": "AKTYWA",
    "Aktywa_A": "A. Aktywa trwale",
    "Aktywa_A_I": "I. Wartosci niematerialne i prawne",
    "Aktywa_A_I_1": "1. Koszty zakonczonych prac rozwojowych",
    "Aktywa_A_I_2": "2. Wartosc firmy",
    "Aktywa_A_I_3": "3. Inne wartosci niematerialne i prawne",
    "Aktywa_A_I_4": "4. Zaliczki na wartosci niematerialne i prawne",
    "Aktywa_A_II": "II. Rzeczowe aktywa trwale",
    "Aktywa_A_II_1": "1. Srodki trwale",
    "Aktywa_A_II_1_A": "a) grunty (w tym prawo uzytkowania wieczystego gruntu)",
    "Aktywa_A_II_1_B": "b) budynki, lokale, prawa do lokali i obiekty inzynierii ladowej i wodnej",
    "Aktywa_A_II_1_C": "c) urzadzenia techniczne i maszyny",
    "Aktywa_A_II_1_D": "d) srodki transportu",
    "Aktywa_A_II_1_E": "e) inne srodki trwale",
    "Aktywa_A_II_2": "2. Srodki trwale w budowie",
    "Aktywa_A_II_3": "3. Zaliczki na srodki trwale w budowie",
    "Aktywa_A_III": "III. Naleznosci dlugoterminowe",
    "Aktywa_A_III_1": "1. Od jednostek powiazanych",
    "Aktywa_A_III_2": "2. Od pozostalych jednostek, w ktorych jednostka posiada zaangazowanie w kapitale",
    "Aktywa_A_III_3": "3. Od pozostalych jednostek",
    "Aktywa_A_IV": "IV. Inwestycje dlugoterminowe",
    "Aktywa_A_IV_1": "1. Nieruchomosci",
    "Aktywa_A_IV_2": "2. Wartosci niematerialne i prawne",
    "Aktywa_A_IV_3": "3. Dlugoterminowe aktywa finansowe",
    "Aktywa_A_IV_3_A": "a) w jednostkach powiazanych",
    "Aktywa_A_IV_3_A_1": "- udzialy lub akcje",
    "Aktywa_A_IV_3_A_2": "- inne papiery wartosciowe",
    "Aktywa_A_IV_3_A_3": "- udzielone pozyczki",
    "Aktywa_A_IV_3_A_4": "- inne dlugoterminowe aktywa finansowe",
    "Aktywa_A_IV_3_B": "b) w pozostalych jednostkach, w ktorych jednostka posiada zaangazowanie w kapitale",
    "Aktywa_A_IV_3_B_1": "- udzialy lub akcje",
    "Aktywa_A_IV_3_B_2": "- inne papiery wartosciowe",
    "Aktywa_A_IV_3_B_3": "- udzielone pozyczki",
    "Aktywa_A_IV_3_B_4": "- inne dlugoterminowe aktywa finansowe",
    "Aktywa_A_IV_3_C": "c) w pozostalych jednostkach",
    "Aktywa_A_IV_3_C_1": "- udzialy lub akcje",
    "Aktywa_A_IV_3_C_2": "- inne papiery wartosciowe",
    "Aktywa_A_IV_3_C_3": "- udzielone pozyczki",
    "Aktywa_A_IV_3_C_4": "- inne dlugoterminowe aktywa finansowe",
    "Aktywa_A_IV_4": "4. Inne inwestycje dlugoterminowe",
    "Aktywa_A_V": "V. Dlugoterminowe rozliczenia miedzyokresowe",
    "Aktywa_A_V_1": "1. Aktywa z tytulu odroczonego podatku dochodowego",
    "Aktywa_A_V_2": "2. Inne rozliczenia miedzyokresowe",
    "Aktywa_B": "B. Aktywa obrotowe",
    "Aktywa_B_I": "I. Zapasy",
    "Aktywa_B_I_1": "1. Materialy",
    "Aktywa_B_I_2": "2. Polprodukty i produkty w toku",
    "Aktywa_B_I_3": "3. Produkty gotowe",
    "Aktywa_B_I_4": "4. Towary",
    "Aktywa_B_I_5": "5. Zaliczki na dostawy i uslugi",
    "Aktywa_B_II": "II. Naleznosci krotkoterminowe",
    "Aktywa_B_II_1": "1. Naleznosci od jednostek powiazanych",
    "Aktywa_B_II_1_A": "a) z tytulu dostaw i uslug",
    "Aktywa_B_II_1_A_1": "- o okresie splaty do 12 miesiecy",
    "Aktywa_B_II_1_A_2": "- o okresie splaty powyzej 12 miesiecy",
    "Aktywa_B_II_1_B": "b) inne",
    "Aktywa_B_II_2": "2. Naleznosci od pozostalych jednostek, w ktorych jednostka posiada zaangazowanie w kapitale",
    "Aktywa_B_II_2_A": "a) z tytulu dostaw i uslug",
    "Aktywa_B_II_2_A_1": "- o okresie splaty do 12 miesiecy",
    "Aktywa_B_II_2_A_2": "- o okresie splaty powyzej 12 miesiecy",
    "Aktywa_B_II_2_B": "b) inne",
    "Aktywa_B_II_3": "3. Naleznosci od pozostalych jednostek",
    "Aktywa_B_II_3_A": "a) z tytulu dostaw i uslug",
    "Aktywa_B_II_3_A_1": "- o okresie splaty do 12 miesiecy",
    "Aktywa_B_II_3_A_2": "- o okresie splaty powyzej 12 miesiecy",
    "Aktywa_B_II_3_B": "b) z tytulu podatkow, dotacji, cel, ubezpieczen spolecznych i zdrowotnych oraz innych tyt. publicznoprawnych",
    "Aktywa_B_II_3_C": "c) inne",
    "Aktywa_B_II_3_D": "d) dochodzone na drodze sadowej",
    "Aktywa_B_III": "III. Inwestycje krotkoterminowe",
    "Aktywa_B_III_1": "1. Krotkoterminowe aktywa finansowe",
    "Aktywa_B_III_1_A": "a) w jednostkach powiazanych",
    "Aktywa_B_III_1_A_1": "- udzialy lub akcje",
    "Aktywa_B_III_1_A_2": "- inne papiery wartosciowe",
    "Aktywa_B_III_1_A_3": "- udzielone pozyczki",
    "Aktywa_B_III_1_A_4": "- inne krotkoterminowe aktywa finansowe",
    "Aktywa_B_III_1_B": "b) w pozostalych jednostkach",
    "Aktywa_B_III_1_B_1": "- udzialy lub akcje",
    "Aktywa_B_III_1_B_2": "- inne papiery wartosciowe",
    "Aktywa_B_III_1_B_3": "- udzielone pozyczki",
    "Aktywa_B_III_1_B_4": "- inne krotkoterminowe aktywa finansowe",
    "Aktywa_B_III_1_C": "c) srodki pieniezne i inne aktywa pieniezne",
    "Aktywa_B_III_1_C_1": "- srodki pieniezne w kasie i na rachunkach",
    "Aktywa_B_III_1_C_2": "- inne srodki pieniezne",
    "Aktywa_B_III_1_C_3": "- inne aktywa pieniezne",
    "Aktywa_B_III_2": "2. Inne inwestycje krotkoterminowe",
    "Aktywa_B_IV": "IV. Krotkoterminowe rozliczenia miedzyokresowe",
    "Aktywa_C": "C. Nalezne wplaty na kapital (fundusz) podstawowy",
    "Aktywa_D": "D. Udzialy (akcje) wlasne",

    # === BILANS - PASYWA ===
    "Pasywa": "PASYWA",
    "Pasywa_A": "A. Kapital (fundusz) wlasny",
    "Pasywa_A_I": "I. Kapital (fundusz) podstawowy",
    "Pasywa_A_II": "II. Kapital (fundusz) zapasowy, w tym nadwyzka wartosci sprzedazy (wartosci emisyjnej) nad wartoscia nominalna udzialow (akcji)",
    "Pasywa_A_II_1": "(w tym: nadwyzka wartosci sprzedazy nad nominalna)",
    "Pasywa_A_III": "III. Kapital (fundusz) z aktualizacji wyceny, w tym z tytulu trwalej utraty wartosci",
    "Pasywa_A_III_1": "(w tym: z tytulu trwalej utraty wartosci)",
    "Pasywa_A_IV": "IV. Pozostale kapitaly (fundusze) rezerwowe, w tym tworzone zgodnie z umowa (statutem) spolki",
    "Pasywa_A_IV_1": "(w tym: tworzone zgodnie z umowa spolki)",
    "Pasywa_A_IV_2": "(w tym: na udzialy (akcje) wlasne)",
    "Pasywa_A_V": "V. Zysk (strata) z lat ubieglych",
    "Pasywa_A_VI": "VI. Zysk (strata) netto",
    "Pasywa_A_VII": "VII. Odpisy z zysku netto w ciagu roku obrotowego (wartosc ujemna)",
    "Pasywa_B": "B. Zobowiazania i rezerwy na zobowiazania",
    "Pasywa_B_I": "I. Rezerwy na zobowiazania",
    "Pasywa_B_I_1": "1. Rezerwa z tytulu odroczonego podatku dochodowego",
    "Pasywa_B_I_2": "2. Rezerwa na swiadczenia emerytalne i podobne",
    "Pasywa_B_I_2_1": "a) dlugoterminowa",
    "Pasywa_B_I_2_2": "b) krotkoterminowa",
    "Pasywa_B_I_3": "3. Pozostale rezerwy",
    "Pasywa_B_I_3_1": "a) dlugoterminowe",
    "Pasywa_B_I_3_2": "b) krotkoterminowe",
    "Pasywa_B_II": "II. Zobowiazania dlugoterminowe",
    "Pasywa_B_II_1": "1. Wobec jednostek powiazanych",
    "Pasywa_B_II_2": "2. Wobec pozostalych jednostek, w ktorych jednostka posiada zaangazowanie w kapitale",
    "Pasywa_B_II_3": "3. Wobec pozostalych jednostek",
    "Pasywa_B_II_3_A": "a) kredyty i pozyczki",
    "Pasywa_B_II_3_B": "b) z tytulu emisji dluznych papierow wartosciowych",
    "Pasywa_B_II_3_C": "c) inne zobowiazania finansowe",
    "Pasywa_B_II_3_D": "d) zobowiazania wekslowe",
    "Pasywa_B_II_3_E": "e) inne",
    "Pasywa_B_III": "III. Zobowiazania krotkoterminowe",
    "Pasywa_B_III_1": "1. Zobowiazania wobec jednostek powiazanych",
    "Pasywa_B_III_1_A": "a) z tytulu dostaw i uslug",
    "Pasywa_B_III_1_A_1": "- o okresie wymagalnosci do 12 miesiecy",
    "Pasywa_B_III_1_A_2": "- o okresie wymagalnosci powyzej 12 miesiecy",
    "Pasywa_B_III_1_B": "b) inne",
    "Pasywa_B_III_2": "2. Zobowiazania wobec pozostalych jednostek, w ktorych jednostka posiada zaangazowanie w kapitale",
    "Pasywa_B_III_2_A": "a) z tytulu dostaw i uslug",
    "Pasywa_B_III_2_A_1": "- o okresie wymagalnosci do 12 miesiecy",
    "Pasywa_B_III_2_A_2": "- o okresie wymagalnosci powyzej 12 miesiecy",
    "Pasywa_B_III_2_B": "b) inne",
    "Pasywa_B_III_3": "3. Zobowiazania wobec pozostalych jednostek",
    "Pasywa_B_III_3_A": "a) kredyty i pozyczki",
    "Pasywa_B_III_3_B": "b) z tytulu emisji dluznych papierow wartosciowych",
    "Pasywa_B_III_3_C": "c) inne zobowiazania finansowe",
    "Pasywa_B_III_3_D": "d) z tytulu dostaw i uslug",
    "Pasywa_B_III_3_D_1": "- o okresie wymagalnosci do 12 miesiecy",
    "Pasywa_B_III_3_D_2": "- o okresie wymagalnosci powyzej 12 miesiecy",
    "Pasywa_B_III_3_E": "e) zaliczki otrzymane na dostawy i uslugi",
    "Pasywa_B_III_3_F": "f) zobowiazania wekslowe",
    "Pasywa_B_III_3_G": "g) z tytulu podatkow, cel, ubezpieczen spolecznych i zdrowotnych oraz innych tytul. publicznoprawnych",
    "Pasywa_B_III_3_H": "h) z tytulu wynagrodzen",
    "Pasywa_B_III_3_I": "i) inne",
    "Pasywa_B_III_4": "4. Fundusze specjalne",
    "Pasywa_B_IV": "IV. Rozliczenia miedzyokresowe",
    "Pasywa_B_IV_1": "1. Ujemna wartosc firmy",
    "Pasywa_B_IV_2": "2. Inne rozliczenia miedzyokresowe",
    "Pasywa_B_IV_2_1": "a) dlugoterminowe",
    "Pasywa_B_IV_2_2": "b) krotkoterminowe",

    # === RZiS POROWNAWCZY ===
    "A": "A. Przychody netto ze sprzedazy i zrownane z nimi",
    "A_J": "(w tym: od jednostek powiazanych)",
    "A_I": "I. Przychody netto ze sprzedazy produktow",
    "A_II": "II. Zmiana stanu produktow (zwiekszenie - wartosc dodatnia, zmniejszenie - wartosc ujemna)",
    "A_III": "III. Koszt wytworzenia produktow na wlasne potrzeby jednostki",
    "A_IV": "IV. Przychody netto ze sprzedazy towarow i materialow",
    "B": "B. Koszty dzialalnosci operacyjnej",
    "B_I": "I. Amortyzacja",
    "B_II": "II. Zuzycie materialow i energii",
    "B_III": "III. Uslugi obce",
    "B_IV": "IV. Podatki i oplaty, w tym podatek akcyzowy",
    "B_IV_1": "(w tym: podatek akcyzowy)",
    "B_V": "V. Wynagrodzenia",
    "B_VI": "VI. Ubezpieczenia spoleczne i inne swiadczenia, w tym emerytalne",
    "B_VI_1": "(w tym: emerytalne)",
    "B_VII": "VII. Pozostale koszty rodzajowe",
    "B_VIII": "VIII. Wartosc sprzedanych towarow i materialow",
    "C": "C. Zysk (strata) ze sprzedazy (A-B)",
    "D": "D. Pozostale przychody operacyjne",
    "D_I": "I. Zysk z tytulu rozchodu niefinansowych aktywow trwalych",
    "D_II": "II. Dotacje",
    "D_III": "III. Aktualizacja wartosci aktywow niefinansowych",
    "D_IV": "IV. Inne przychody operacyjne",
    "E": "E. Pozostale koszty operacyjne",
    "E_I": "I. Strata z tytulu rozchodu niefinansowych aktywow trwalych",
    "E_II": "II. Aktualizacja wartosci aktywow niefinansowych",
    "E_III": "III. Inne koszty operacyjne",
    "F": "F. Zysk (strata) z dzialalnosci operacyjnej (C+D-E)",
    "G": "G. Przychody finansowe",
    "G_I": "I. Dywidendy i udzialy w zyskach, w tym od jednostek powiazanych",
    "G_I_A": "a) od jednostek powiazanych",
    "G_I_A_1": "(w tym: w ktorych jednostka posiada zaangazowanie w kapitale)",
    "G_I_B": "b) od pozostalych jednostek",
    "G_I_B_1": "(w tym: w ktorych jednostka posiada zaangazowanie w kapitale)",
    "G_II": "II. Odsetki, w tym od jednostek powiazanych",
    "G_II_J": "(w tym: od jednostek powiazanych)",
    "G_III": "III. Zysk z tytulu rozchodu aktywow finansowych, w tym w jednostkach powiazanych",
    "G_III_J": "(w tym: w jednostkach powiazanych)",
    "G_IV": "IV. Aktualizacja wartosci aktywow finansowych",
    "G_V": "V. Inne",
    "H": "H. Koszty finansowe",
    "H_I": "I. Odsetki, w tym dla jednostek powiazanych",
    "H_I_J": "(w tym: dla jednostek powiazanych)",
    "H_II": "II. Strata z tytulu rozchodu aktywow finansowych, w tym w jednostkach powiazanych",
    "H_II_J": "(w tym: w jednostkach powiazanych)",
    "H_III": "III. Aktualizacja wartosci aktywow finansowych",
    "H_IV": "IV. Inne",
    "I": "I. Zysk (strata) brutto (F+G-H)",
    "J": "J. Podatek dochodowy",
    "K": "K. Pozostale obowiazkowe zmniejszenia zysku (zwiekszenia straty)",
    "L": "L. Zysk (strata) netto (I-J-K)",

    # === RACHUNEK PRZEPLYWOW PIENIEZNYCH (metoda posrednia) ===
    # NOTE: Cash flow tags inside PrzeplywyPosr do NOT have a prefix.
    # Use context (which section you're in) to disambiguate from RZiS tags.
    # In the API response, prefix with "CF." to avoid collisions.
    "CF.A_I": "I. Zysk (strata) netto",
    "CF.A_II": "II. Korekty razem",
    "CF.A_II_1": "1. Amortyzacja",
    "CF.A_II_2": "2. Zyski (straty) z tytulu roznic kursowych",
    "CF.A_II_3": "3. Odsetki i udzialy w zyskach (dywidendy)",
    "CF.A_II_4": "4. Zysk (strata) z dzialalnosci inwestycyjnej",
    "CF.A_II_5": "5. Zmiana stanu rezerw",
    "CF.A_II_6": "6. Zmiana stanu zapasow",
    "CF.A_II_7": "7. Zmiana stanu naleznosci",
    "CF.A_II_8": "8. Zmiana stanu zobowiazan krotkoterminowych (z wyjatkiem pozyczek i kredytow)",
    "CF.A_II_9": "9. Zmiana stanu rozliczen miedzyokresowych",
    "CF.A_II_10": "10. Inne korekty",
    "CF.A_III": "III. Przeplyw pieniezny netto z dzialalnosci operacyjnej (I +/- II)",
    "CF.B_I": "I. Wplywy",
    "CF.B_I_1": "1. Zbycie wartosci niematerialnych i prawnych oraz rzeczowych aktywow trwalych",
    "CF.B_I_2": "2. Zbycie inwestycji w nieruchomosci oraz wartosci niematerialne i prawne",
    "CF.B_I_3": "3. Z aktywow finansowych",
    "CF.B_I_3_A": "a) w jednostkach powiazanych",
    "CF.B_I_3_B": "b) w pozostalych jednostkach",
    "CF.B_I_3_B_1": "- zbycie aktywow finansowych",
    "CF.B_I_3_B_2": "- dywidendy i udzialy w zyskach",
    "CF.B_I_3_B_3": "- splata udzielonych pozyczek dlugoterminowych",
    "CF.B_I_3_B_4": "- odsetki",
    "CF.B_I_3_B_5": "- inne wplywy z aktywow finansowych",
    "CF.B_I_4": "4. Inne wplywy inwestycyjne",
    "CF.B_II": "II. Wydatki",
    "CF.B_II_1": "1. Nabycie wartosci niematerialnych i prawnych oraz rzeczowych aktywow trwalych",
    "CF.B_II_2": "2. Inwestycje w nieruchomosci oraz wartosci niematerialne i prawne",
    "CF.B_II_3": "3. Na aktywa finansowe",
    "CF.B_II_4": "4. Inne wydatki inwestycyjne",
    "CF.B_III": "III. Przeplyw pieniezny netto z dzialalnosci inwestycyjnej (I-II)",
    "CF.C_I": "I. Wplywy",
    "CF.C_I_1": "1. Wplywy netto z wydania udzialow (emisji akcji) i innych instrumentow kapitalowych oraz doplat do kapitalu",
    "CF.C_I_2": "2. Kredyty i pozyczki",
    "CF.C_I_3": "3. Emisja dluznych papierow wartosciowych",
    "CF.C_I_4": "4. Inne wplywy finansowe",
    "CF.C_II": "II. Wydatki",
    "CF.C_II_1": "1. Nabycie udzialow (akcji) wlasnych",
    "CF.C_II_2": "2. Dywidendy i inne wyplaty na rzecz wlascicieli",
    "CF.C_II_3": "3. Inne niz wyplaty na rzecz wlascicieli wydatki z tytulu podzialu zysku",
    "CF.C_II_4": "4. Splaty kredytow i pozyczek",
    "CF.C_II_5": "5. Wykup dluznych papierow wartosciowych",
    "CF.C_II_6": "6. Z tytulu innych zobowiazan finansowych",
    "CF.C_II_7": "7. Platnosci zobowiazan z tytulu umow leasingu finansowego",
    "CF.C_II_8": "8. Odsetki",
    "CF.C_II_9": "9. Inne wydatki finansowe",
    "CF.C_III": "III. Przeplyw pieniezny netto z dzialalnosci finansowej (I-II)",
    "CF.D": "D. Przeplyw pieniezny netto razem (A.III +/- B.III +/- C.III)",
    "CF.E": "E. Bilansowa zmiana stanu srodkow pienieznych, w tym",
    "CF.E_1": "(w tym: zmiana stanu srodkow pienieznych z tytulu roznic kursowych)",
    "CF.F": "F. Srodki pieniezne na poczatek okresu",
    "CF.G": "G. Srodki pieniezne na koniec okresu (F +/- D), w tym",
    "CF.G_1": "(w tym: o ograniczonej mozliwosci dysponowania)",
}
```

---

## Caching Strategy

Parsing XML is cheap but downloading from the upstream API is slow (2-5s per file). Implement a simple in-memory cache (or Redis if available):

- Cache key: `{krs}:{document_id}` -> parsed `FinancialNode` tree
- TTL: 1 hour (financial statements don't change once filed)
- The `/available-periods` endpoint should also cache the document list per KRS

---

## Testing

Use KRS `0000694720` (B-JWK-Management) as the primary test case. The 2024 statement is available and has all sections (Bilans, RZiS porownawczy, Przeplyw posredni).

Expected test assertions for the 2024 statement:
- `Aktywa.kwota_a == 115365276.82`
- `Aktywa.kwota_b == 162710400.36`
- `Pasywa.kwota_a == Aktywa.kwota_a` (balance sheet must balance)
- `RZiS.L.kwota_a == 21084025.47` (net profit)
- Tree depth for Bilans should be at least 6 levels (Aktywa > B > III > 1 > C > 1)
- Cash flow A_III.kwota_a should equal RZiS L.kwota_a (net profit flows into cash flow)
