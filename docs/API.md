# RDF API Proxy — API Reference

> Agent-facing documentation. Everything a coding agent needs to call this service,
> handle responses, and build on top of it.

---

## Base URL

```
http://localhost:8000
```

Interactive Swagger UI: `http://localhost:8000/docs`

---

## Overview

This is an async FastAPI proxy that sits in front of the Polish Ministry of Justice
financial document registry (`rdf-przegladarka.ms.gov.pl`). It handles KRS encryption,
translates Polish field names to English snake_case, and exposes a clean JSON API.

**Typical flow:**

```
1. POST /api/podmiot/lookup                      — validate KRS, get company name
2. POST /api/podmiot/document-types              — list available document categories (optional)
3. POST /api/dokumenty/search                    — paginated list of documents with IDs
4. GET  /api/dokumenty/metadata/{id}             — full metadata for a single document
5. POST /api/dokumenty/download                  — download one or more documents as ZIP
```

**Analysis flow (XML parsing — server-side, no client download required):**

```
1. GET  /api/analysis/available-periods/{krs}    — list all parseable periods for a company
2. POST /api/analysis/statement                  — parse a single statement into a full tree
3. POST /api/analysis/compare                    — YoY comparison with change % and ratios
4. POST /api/analysis/time-series                — track specific fields across multiple years
```

> Analysis endpoints only work for Polish GAAP documents (`czyMSR = false`, `rodzaj = "18"`).
> IFRS/XHTML statements are filtered out automatically.

---

## Endpoints

---

### GET /health

Liveness check. No authentication required.

**Response `200`**
```json
{ "status": "ok" }
```

---

### POST /api/podmiot/lookup

Validate a KRS number and retrieve basic company information.

**Request body**
```json
{ "krs": "694720" }
```

| Field | Type   | Rules                    | Description              |
|-------|--------|--------------------------|--------------------------|
| `krs` | string | digits only, 1–10 chars  | KRS number (leading zeros optional — `"694720"` and `"0000694720"` are both accepted) |

**Response `200`**
```json
{
  "podmiot": {
    "numer_krs": "0000694720",
    "nazwa_podmiotu": "B-JWK-MANAGEMENT SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ",
    "forma_prawna": "SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ",
    "wykreslenie": ""
  },
  "czy_podmiot_znaleziony": true,
  "komunikat_bledu": null
}
```

| Field                    | Type           | Description |
|--------------------------|----------------|-------------|
| `podmiot`                | object \| null | `null` when not found |
| `podmiot.numer_krs`      | string         | Zero-padded 10-digit KRS |
| `podmiot.nazwa_podmiotu` | string         | Full legal company name |
| `podmiot.forma_prawna`   | string         | Legal form (e.g. "SPÓŁKA Z O.O.") |
| `podmiot.wykreslenie`    | string         | Deregistration info; empty string `""` if the company is active |
| `czy_podmiot_znaleziony` | boolean        | `false` → KRS does not exist in registry |
| `komunikat_bledu`        | string \| null | Error message from upstream; `null` on success |

**When company is not found:**
```json
{
  "podmiot": null,
  "czy_podmiot_znaleziony": false,
  "komunikat_bledu": "Podmiot nie znaleziony"
}
```

---

### POST /api/podmiot/document-types

List document categories available for a company.

**Request body**
```json
{ "krs": "694720" }
```

**Response `200`** — array of document type names
```json
[
  { "nazwa": "Roczne sprawozdanie finansowe" },
  { "nazwa": "Uchwała o podziale zysku bądź pokryciu straty" },
  { "nazwa": "Uchwała lub postanowienie o zatwierdzeniu rocznego sprawozdania finansowego" },
  { "nazwa": "Sprawozdanie z działalności" },
  { "nazwa": "Opinia biegłego rewidenta / Sprawozdanie z badania rocznego sprawozdania finansowego" }
]
```

| `nazwa` (PL)                                                                                 | Meaning (EN)                        |
|----------------------------------------------------------------------------------------------|-------------------------------------|
| Roczne sprawozdanie finansowe                                                                | Annual financial statement          |
| Uchwała o podziale zysku bądź pokryciu straty                                                | Profit distribution resolution      |
| Uchwała lub postanowienie o zatwierdzeniu rocznego sprawozdania finansowego                  | Resolution approving annual FS      |
| Sprawozdanie z działalności                                                                  | Management / activity report        |
| Opinia biegłego rewidenta / Sprawozdanie z badania rocznego sprawozdania finansowego         | Auditor's opinion / audit report    |

---

### POST /api/dokumenty/search

Paginated list of all documents filed by a company. Returns document IDs needed for
metadata and download calls.

**Request body**
```json
{
  "krs": "694720",
  "page": 0,
  "page_size": 10,
  "sort_field": "id",
  "sort_dir": "MALEJACO"
}
```

| Field        | Type    | Default      | Rules         | Description |
|--------------|---------|--------------|---------------|-------------|
| `krs`        | string  | required     | digits, 1–10  | KRS number |
| `page`       | integer | `0`          | >= 0          | 0-indexed page number |
| `page_size`  | integer | `10`         | 1–100         | Results per page |
| `sort_field` | string  | `"id"`       | —             | Field to sort by (`"id"` is the only observed value) |
| `sort_dir`   | string  | `"MALEJACO"` | `"MALEJACO"` \| `"ROSNACO"` | `"MALEJACO"` = descending (newest first), `"ROSNACO"` = ascending |

**Response `200`**
```json
{
  "content": [
    {
      "id": "ZgsX8Fsncb1PFW07-T4XoQ==",
      "rodzaj": "18",
      "status": "NIEUSUNIETY",
      "status_bezpieczenstwa": null,
      "nazwa": null,
      "okres_sprawozdawczy_poczatek": "2024-01-01",
      "okres_sprawozdawczy_koniec": "2024-12-31",
      "data_usuniecia_dokumentu": ""
    }
  ],
  "metadane_wynikow": {
    "numer_strony": 0,
    "rozmiar_strony": 10,
    "liczba_stron": 4,
    "calkowita_liczba_obiektow": 31
  }
}
```

**`content[]` fields**

| Field                          | Type           | Description |
|--------------------------------|----------------|-------------|
| `id`                           | string         | Document ID (Base64). Pass this to `/metadata/{id}` and `/download`. |
| `rodzaj`                       | string         | Document type code — see table below |
| `status`                       | string         | `"NIEUSUNIETY"` = active (not deleted) |
| `status_bezpieczenstwa`        | string \| null | Security status; usually `null` |
| `nazwa`                        | string \| null | Document name; usually `null` |
| `okres_sprawozdawczy_poczatek` | string         | Reporting period start `YYYY-MM-DD` |
| `okres_sprawozdawczy_koniec`   | string         | Reporting period end `YYYY-MM-DD` |
| `data_usuniecia_dokumentu`     | string         | Deletion date; empty string `""` if not deleted |

**`rodzaj` codes**

| Code | Document type (EN)                  |
|------|-------------------------------------|
| `3`  | Approval resolution                 |
| `4`  | Profit distribution resolution      |
| `18` | Annual financial statement          |
| `19` | Management / activity report        |
| `20` | Auditor's opinion                   |

**`metadane_wynikow` fields**

| Field                       | Type    | Description |
|-----------------------------|---------|-------------|
| `numer_strony`              | integer | Current page (0-indexed) |
| `rozmiar_strony`            | integer | Page size used |
| `liczba_stron`              | integer | Total number of pages |
| `calkowita_liczba_obiektow` | integer | Total document count |

---

### GET /api/dokumenty/metadata/{doc_id}

Full metadata for a single document.

**Path parameter**

| Parameter | Description |
|-----------|-------------|
| `doc_id`  | Document ID as returned in `content[].id` from `/search`. Pass it **as-is** — the proxy handles URL-encoding of Base64 characters (`=`, `+`, `/`). |

**Example**
```
GET /api/dokumenty/metadata/ZgsX8Fsncb1PFW07-T4XoQ==
```

**Response `200`** — raw upstream object (Polish field names, passed through unchanged)
```json
{
  "rodzajDokumentu": {
    "id": 18,
    "kodKRS": "SF",
    "kod": "30",
    "nazwa": "Roczne sprawozdanie finansowe",
    "validFrom": "2018-01-10",
    "validTo": null,
    "standardMSR": true,
    "podpisWymagany": true,
    "dopuszczalneRozszerzenia": ".XML, .XAdES, .XML.SIG, .SIG, .p7m",
    "dopuszczalneRozszerzeniaMSR": ".ZIP, .XHTML, .XML, .PDF, .JPG"
  },
  "status": "NIEUSUNIETY",
  "nazwa": "",
  "okresSprawozdawczyPoczatek": "2024-01-01",
  "okresSprawozdawczyKoniec": "2024-12-31",
  "dataSporzadzenia": "2025-04-28",
  "dataDodania": "2025-05-20",
  "czyMSR": false,
  "czyKorekta": false,
  "identyfikator": "ZgsX8Fsncb1PFW07-T4XoQ==",
  "nrKRS": "0000694720",
  "formaPrawna": "SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ",
  "nazwaPodmiotu": "B-JWK-MANAGEMENT SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ",
  "jezyk": "polski",
  "nazwaPliku": "Bjwk SF za 2024.xml",
  "uuidDokumentuCRDP": "2F07D70A-646C-420F-BA64-8BCF39E8FD82",
  "idDokumentu": "9530163"
}
```

**Key fields to check before downloading**

| Field             | Type    | Description |
|-------------------|---------|-------------|
| `czyMSR`          | boolean | `false` = Polish GAAP (structured XML); `true` = IFRS (XHTML/iXBRL — harder to parse) |
| `czyKorekta`      | boolean | `true` = this is a correction/amendment of an earlier filing |
| `nazwaPliku`      | string  | Original filename — reveals format: `.xml`, `.xhtml`, `.zip`, `.pdf` |
| `dataSporzadzenia`| string  | Date the document was prepared |
| `dataDodania`     | string  | Date filed to the registry |

> **Note:** This endpoint returns the raw upstream response without field name translation.
> Field names are in Polish camelCase.

---

### POST /api/dokumenty/download

Download one or more documents as a single ZIP archive.

**Request body**
```json
{
  "document_ids": ["ZgsX8Fsncb1PFW07-T4XoQ=="]
}
```

| Field          | Type          | Rules      | Description |
|----------------|---------------|------------|-------------|
| `document_ids` | array[string] | 1–20 items | Document IDs from `/search` |

**Response `200`**

- `Content-Type: application/zip`
- `Content-Disposition: attachment; filename=documents.zip`
- Body: binary ZIP file

The ZIP contains the original document files as submitted by the company (`.xml`, `.xhtml`, `.pdf`, etc.).

**Example — save ZIP in Python**
```python
import httpx

resp = httpx.post(
    "http://localhost:8000/api/dokumenty/download",
    json={"document_ids": ["ZgsX8Fsncb1PFW07-T4XoQ=="]}
)
with open("documents.zip", "wb") as f:
    f.write(resp.content)
```

**Example — extract without saving**
```python
import zipfile, io

with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
    for name in zf.namelist():
        print(name)           # e.g. "Bjwk SF za 2024.xml"
        xml_bytes = zf.read(name)
```

---

---

## Analysis endpoints

These endpoints download and parse Polish GAAP financial statement XMLs server-side.
The client never touches ZIP files or XML — just sends a KRS number and gets structured JSON back.

**Caching:** parsed statements are cached in-memory for 1 hour (keyed by document ID).
Repeat calls within that window are instant.

**Scope:** only `rodzaj = "18"` (annual financial statement), `status = "NIEUSUNIETY"`,
`czyMSR = false` (Polish GAAP). IFRS documents are silently excluded.

---

### GET /api/analysis/available-periods/{krs}

List all available annual financial statement periods for a company.
Does **not** download XML — only calls `/search` + `/metadata`.
Use this to populate a year-selector in a UI before calling the heavier endpoints.

**Path parameter:** `krs` — KRS number (1–10 digits)

**Response `200`**
```json
{
  "krs": "0000694720",
  "company_name": "B-JWK-MANAGEMENT SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ",
  "periods": [
    {
      "period_start": "2019-01-01",
      "period_end": "2019-12-31",
      "document_id": "Bj4U6NqM2-gdavchRK5COw==",
      "date_filed": "2020-10-09",
      "is_correction": false,
      "is_ifrs": false,
      "filename": "Bjwk SF za 2019.xml"
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

| Field             | Type    | Description |
|-------------------|---------|-------------|
| `period_start`    | string  | Reporting period start `YYYY-MM-DD` |
| `period_end`      | string  | Reporting period end `YYYY-MM-DD` — use this as the key for other analysis endpoints |
| `document_id`     | string  | Base64 document ID |
| `date_filed`      | string  | Date filed to registry |
| `is_correction`   | boolean | `true` if this supersedes an earlier filing for the same period |
| `is_ifrs`         | boolean | Always `false` (IFRS excluded) |
| `filename`        | string  | Original XML filename |

> If multiple filings exist for the same period, the storage layer keeps all filings. Read paths that need one record per period should prefer the latest correction.

---

### POST /api/analysis/statement

Parse a single annual financial statement and return the full hierarchical tree
(balance sheet, income statement, cash flow).

**Request body**
```json
{
  "krs": "694720",
  "period_end": "2024-12-31"
}
```

| Field        | Type   | Required | Description |
|--------------|--------|----------|-------------|
| `krs`        | string | yes      | KRS number |
| `period_end` | string | no       | `YYYY-MM-DD` period end date. Omit to get the most recent statement. |

**Response `200`**
```json
{
  "company": {
    "name": "B-JWK-Management Spółka z ograniczoną odpowiedzialnością",
    "krs": "0000694720",
    "nip": "5842734981",
    "pkd": "6810Z",
    "period_start": "2024-01-01",
    "period_end": "2024-12-31",
    "date_prepared": "2025-04-28",
    "schema_type": "JednostkaInna",
    "rzis_variant": "porownawczy",
    "cf_method": "posrednia"
  },
  "bilans": {
    "aktywa": {
      "tag": "Aktywa",
      "label": "AKTYWA",
      "kwota_a": 115365276.82,
      "kwota_b": 162710400.36,
      "kwota_b1": null,
      "depth": 0,
      "is_w_tym": false,
      "children": [ ]
    },
    "pasywa": { }
  },
  "rzis": [ ],
  "cash_flow": [ ]
}
```

**`company` fields**

| Field          | Type           | Description |
|----------------|----------------|-------------|
| `schema_type`  | string         | XML root tag: `JednostkaInna` (standard), `JednostkaMikro`, `JednostkaMala`, `JednostkaOPP` |
| `rzis_variant` | string \| null | `"porownawczy"` or `"kalkulacyjny"` — income statement method |
| `cf_method`    | string \| null | `"posrednia"` (indirect) or `"bezposrednia"` (direct). `null` for micro/small entities with no cash flow. |

**`FinancialNode` schema** — every node in `bilans`, `rzis[]`, and `cash_flow[]` follows this shape:

| Field      | Type           | Description |
|------------|----------------|-------------|
| `tag`      | string         | Identifier. Bilans: `Aktywa_A_II_1_B`. Income statement: `RZiS.A`, `RZiS.L`. Cash flow: `CF.A_III`. |
| `label`    | string         | Human-readable Polish label, e.g. `"A. Przychody netto ze sprzedazy..."` |
| `kwota_a`  | float          | Current reporting year value |
| `kwota_b`  | float          | Prior year value (embedded comparison column in the XML) |
| `kwota_b1` | float \| null  | Year before prior, if present |
| `depth`    | integer        | Nesting level (0 = top-level section) |
| `is_w_tym` | boolean        | `true` for "of which" sub-items — NOT additive with siblings |
| `children` | array          | Recursive child nodes |

**Tag naming conventions**

| Section     | Prefix   | Examples |
|-------------|----------|---------|
| Balance sheet assets | none | `Aktywa`, `Aktywa_A`, `Aktywa_B_III_1_C_1` |
| Balance sheet liabilities | none | `Pasywa`, `Pasywa_A`, `Pasywa_B_III` |
| Income statement | `RZiS.` | `RZiS.A`, `RZiS.B`, `RZiS.F`, `RZiS.L` |
| Cash flow | `CF.` | `CF.A_III`, `CF.B_III`, `CF.C_III`, `CF.D` |

**Structure of `bilans`:** object with `aktywa` and `pasywa` keys, each a single root `FinancialNode`.

**Structure of `rzis` and `cash_flow`:** arrays of top-level `FinancialNode` objects (the section has no single root with a value).

> `bilans.aktywa.kwota_a` must equal `bilans.pasywa.kwota_a` — the balance sheet always balances.

---

### POST /api/analysis/compare

Compare two annual financial statements year-over-year. Returns a merged tree with
absolute and percentage changes at every node, plus key financial ratios.

**Request body**
```json
{
  "krs": "694720",
  "period_end_current": "2024-12-31",
  "period_end_previous": "2023-12-31"
}
```

| Field                 | Type   | Required | Description |
|-----------------------|--------|----------|-------------|
| `krs`                 | string | yes      | KRS number |
| `period_end_current`  | string | yes      | The "current" year period end |
| `period_end_previous` | string | yes      | The "previous" year period end |

Both periods are fetched and parsed independently. Can compare any two available years (e.g. 2024 vs 2019).

**Response `200`**
```json
{
  "company": {
    "name": "B-JWK-Management Spółka z ograniczoną odpowiedzialnością",
    "krs": "0000694720",
    "nip": "5842734981"
  },
  "current_period":  { "start": "2024-01-01", "end": "2024-12-31" },
  "previous_period": { "start": "2023-01-01", "end": "2023-12-31" },
  "bilans": {
    "aktywa": {
      "tag": "Aktywa",
      "label": "AKTYWA",
      "current": 115365276.82,
      "previous": 162710400.36,
      "change_absolute": -47345123.54,
      "change_percent": -29.1,
      "share_of_parent_current": null,
      "share_of_parent_previous": null,
      "depth": 0,
      "is_w_tym": false,
      "children": [ ]
    },
    "pasywa": { }
  },
  "rzis": [ ],
  "cash_flow": [ ],
  "ratios": {
    "equity_ratio":        { "current": 0.7473, "previous": 0.4218, "change": 0.3255 },
    "current_ratio":       { "current": 17.1512, "previous": 1.0707, "change": 16.0805 },
    "debt_ratio":          { "current": 0.2527, "previous": 0.5782, "change": -0.3255 },
    "operating_margin":    { "current": 0.5922, "previous": 0.2009, "change": 0.3913 },
    "net_margin":          { "current": 0.4869, "previous": 0.1445, "change": 0.3424 },
    "revenue_change_pct":  13.03,
    "net_profit_change_pct": 280.99
  }
}
```

**`ComparisonNode` schema** — every node in `bilans`, `rzis[]`, `cash_flow[]`:

| Field                      | Type           | Description |
|----------------------------|----------------|-------------|
| `tag`                      | string         | Same tag naming as `FinancialNode` |
| `label`                    | string         | Human-readable Polish label |
| `current`                  | float          | Current year value |
| `previous`                 | float          | Previous year value |
| `change_absolute`          | float          | `current - previous` |
| `change_percent`           | float \| null  | `(current - previous) / abs(previous) * 100`. `null` when `previous == 0`. |
| `share_of_parent_current`  | float \| null  | `current / parent.current * 100`. `null` at root level. |
| `share_of_parent_previous` | float \| null  | `previous / parent.previous * 100`. `null` at root level. |
| `depth`                    | integer        | Nesting level |
| `is_w_tym`                 | boolean        | "Of which" sub-item |
| `children`                 | array          | Recursive `ComparisonNode` list |

**Ratio formulas**

| Ratio                  | Formula | Source tags |
|------------------------|---------|-------------|
| `equity_ratio`         | Equity / Total assets | `Pasywa_A / Aktywa` |
| `current_ratio`        | Current assets / Current liabilities | `Aktywa_B / Pasywa_B_III` |
| `debt_ratio`           | Total liabilities / Total assets | `Pasywa_B / Aktywa` |
| `operating_margin`     | Operating profit / Revenue | `RZiS.F / RZiS.A` |
| `net_margin`           | Net profit / Revenue | `RZiS.L / RZiS.A` |
| `revenue_change_pct`   | YoY revenue change % | `RZiS.A` |
| `net_profit_change_pct`| YoY net profit change % | `RZiS.L` |

Any ratio with a zero denominator returns `null`.

---

### POST /api/analysis/time-series

Track selected financial fields across multiple years for trend analysis.

**Request body**
```json
{
  "krs": "694720",
  "fields": ["Aktywa", "Pasywa_A", "RZiS.A", "RZiS.L", "CF.A_III"],
  "period_ends": ["2022-12-31", "2023-12-31", "2024-12-31"]
}
```

| Field         | Type          | Required | Description |
|---------------|---------------|----------|-------------|
| `krs`         | string        | yes      | KRS number |
| `fields`      | array[string] | yes      | Tag names to track. Use the same naming convention as `FinancialNode.tag` (`Aktywa_B`, `RZiS.L`, `CF.A_III`, etc.) |
| `period_ends` | array[string] | no       | Filter to specific period end dates. Omit to return all available years. |

**Response `200`**
```json
{
  "company": {
    "name": "B-JWK-Management Spółka z ograniczoną odpowiedzialnością",
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
      "values":           [176488460.6, 162710400.36, 115365276.82],
      "changes_absolute": [null,        -13778060.24, -47345123.54],
      "changes_percent":  [null,        -7.81,        -29.1]
    },
    {
      "tag": "RZiS.L",
      "label": "L. Zysk (strata) netto (I-J-K)",
      "section": "rzis",
      "values":           [9960811.57, 5533964.74, 21084025.47],
      "changes_absolute": [null,       -4426846.83, 15550060.73],
      "changes_percent":  [null,       -44.44,      280.99]
    }
  ]
}
```

**`series[]` fields**

| Field              | Type          | Description |
|--------------------|---------------|-------------|
| `tag`              | string        | The requested field tag |
| `label`            | string        | Human-readable Polish label |
| `section`          | string        | `"bilans"`, `"rzis"`, or `"cash_flow"` |
| `values`           | array[float?] | One value per period. `null` if the field is absent in that year's XML. |
| `changes_absolute` | array[float?] | `values[i] - values[i-1]`. First element always `null`. |
| `changes_percent`  | array[float?] | Percentage change vs prior period. `null` for first element or zero base. |

**Notes:**
- Periods are sorted chronologically (oldest → newest).
- All requested statements are downloaded in parallel; the cache makes repeat calls fast.
- An extra year may appear at the start of `periods` if the oldest downloaded XML contains an embedded prior-year column (`kwota_b`) that is not itself a separate document.
- If a `fields` tag is not found in a particular year's XML (schema difference between years), its value for that period is `null`.

---

## Error responses

### Validation error `422`

Returned when request body fails schema validation.

```json
{
  "detail": [
    {
      "type": "string_pattern_mismatch",
      "loc": ["body", "krs"],
      "msg": "String should match pattern '^\\d{1,10}$'",
      "input": "ABC"
    }
  ]
}
```

### Upstream error `502`

Returned when the upstream government API returns a non-2xx response.

```json
{
  "detail": "Upstream API error",
  "upstream_status": 500,
  "upstream_url": "https://rdf-przegladarka.ms.gov.pl/..."
}
```

---

## Configuration (environment variables)

| Variable          | Default                                                                                        | Description |
|-------------------|------------------------------------------------------------------------------------------------|-------------|
| `RDF_BASE_URL`    | `https://rdf-przegladarka.ms.gov.pl/services/rdf/przegladarka-dokumentow-finansowych`         | Upstream API base URL |
| `RDF_REFERER`     | `https://rdf-przegladarka.ms.gov.pl/wyszukaj-podmiot`                                         | Referer header sent to upstream |
| `RDF_ORIGIN`      | `https://rdf-przegladarka.ms.gov.pl`                                                           | Origin header sent to upstream |
| `REQUEST_TIMEOUT` | `30`                                                                                           | Upstream request timeout in seconds |
| `MAX_CONNECTIONS` | `20`                                                                                           | Max concurrent connections per worker |
| `CORS_ORIGINS`    | `["*"]`                                                                                        | Allowed CORS origins (JSON array) |
| `WORKERS`         | `4`                                                                                            | Uvicorn worker processes |

---

## Running the service

```bash
# Development
uvicorn app.main:app --reload --port 8000

# Production
uvicorn app.main:app --workers 4 --port 8000

# Docker
docker build -t rdf-api .
docker run -p 8000:8000 rdf-api
```

---

## Complete example flow

```python
import httpx

BASE = "http://localhost:8000"

with httpx.Client() as client:
    # 1. Validate company
    r = client.post(f"{BASE}/api/podmiot/lookup", json={"krs": "694720"})
    r.raise_for_status()
    company = r.json()
    assert company["czy_podmiot_znaleziony"], "KRS not found"
    print(company["podmiot"]["nazwa_podmiotu"])

    # 2. List all documents (newest first)
    r = client.post(f"{BASE}/api/dokumenty/search", json={
        "krs": "694720",
        "page": 0,
        "page_size": 100
    })
    docs = r.json()["content"]

    # 3. Find the most recent annual financial statement
    annual_fs = [d for d in docs if d["rodzaj"] == "18" and d["status"] == "NIEUSUNIETY"]
    latest = annual_fs[0]   # already sorted newest first
    print(f"Latest FS: {latest['okres_sprawozdawczy_poczatek']} – {latest['okres_sprawozdawczy_koniec']}")

    # 4. Check metadata (optional — verify it's Polish GAAP XML)
    r = client.get(f"{BASE}/api/dokumenty/metadata/{latest['id']}")
    meta = r.json()
    print(f"Format: {meta['nazwaPliku']}, IFRS: {meta['czyMSR']}")

    # 5. Download
    r = client.post(f"{BASE}/api/dokumenty/download", json={"document_ids": [latest["id"]]})
    with open("sprawozdanie.zip", "wb") as f:
        f.write(r.content)
    print("Saved sprawozdanie.zip")
```
