# Repozytorium Dokumentów Finansowych (RDF) - API Documentation

> **Source:** Reverse-engineered from `rdf-przegladarka.ms.gov.pl` browser network calls.
> **Date:** 2026-03-19
> **Status:** Unofficial / undocumented API - no SLA, may change without notice.

---

## Base URL

```
https://rdf-przegladarka.ms.gov.pl/services/rdf/przegladarka-dokumentow-finansowych
```

## Common Headers (all requests)

```python
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ...",
    "Referer": "https://rdf-przegladarka.ms.gov.pl/wyszukaj-podmiot",
    "Origin": "https://rdf-przegladarka.ms.gov.pl",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}
```

> **Cookie handling:** The server sets a session cookie (e.g. `b8c1f9fe48b973172f71ee596fbb9b3d=...`). Use `requests.Session()` to persist it across calls.

---

## Flow Overview

The typical workflow to download a financial statement given a KRS number:

```
1. dane-podstawowe       POST  - validate KRS number, get entity name
2. rodzajeDokWyszukiwanie POST - get available document types for entity
3. wyszukiwanie           POST - list all documents (paginated), get document IDs
4. {id}/id-dokumentu-i-korekt GET - get document ID + correction chain
5. {id}                   GET  - get full metadata for a specific document
6. tresc                  POST - download document file (ZIP containing XML/PDF/etc.)
```

```
NIP -> (Open API KRS: prs.ms.gov.pl) -> KRS number -> (RDF API below) -> financial XML
```

---

## Endpoints

---

### 1. Dane podstawowe (Basic Entity Data)

Validates a KRS number and returns basic entity info. This is the entry point - always call first.

**`POST /podmioty/wyszukiwanie/dane-podstawowe`**

#### Request

```json
{
    "numerKRS": "0000694720"
}
```

| Field       | Type   | Required | Description                          |
|-------------|--------|----------|--------------------------------------|
| `numerKRS`  | string | yes      | 10-digit KRS number, zero-padded     |

#### Response `200 OK`

```json
{
    "podmiot": {
        "numerKRS": "0000694720",
        "nazwaPodmiotu": "B-JWK-MANAGEMENT SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ",
        "formaPrawna": "SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ",
        "wykreslenie": ""
    },
    "czyPodmiotZnaleziony": true,
    "komunikatBledu": null
}
```

| Field                          | Type    | Description                                      |
|--------------------------------|---------|--------------------------------------------------|
| `podmiot.numerKRS`             | string  | KRS number                                       |
| `podmiot.nazwaPodmiotu`        | string  | Full legal name                                  |
| `podmiot.formaPrawna`          | string  | Legal form (sp. z o.o., S.A., etc.)              |
| `podmiot.wykreslenie`          | string  | Deregistration info - empty string if active      |
| `czyPodmiotZnaleziony`        | boolean | `true` if entity exists in registry              |
| `komunikatBledu`              | string? | Error message, `null` on success                 |

#### Notes

- If `czyPodmiotZnaleziony` is `false`, the KRS number is invalid or not found.
- The `wykreslenie` field is an empty string for active entities.

---

### 2. Rodzaje dokumentów (Document Types)

Returns a list of document types available for the given entity.

**`POST /dokumenty/rodzajeDokWyszukiwanie`**

#### Request

```json
{
    "nrKRS": "0000694720"
}
```

| Field   | Type   | Required | Description                              |
|---------|--------|----------|------------------------------------------|
| `nrKRS` | string | yes      | KRS number (note: field name differs from endpoint 1!) |

#### Response `200 OK`

```json
[
    {"nazwa": "Roczne sprawozdanie finansowe"},
    {"nazwa": "Uchwała o podziale zysku bądź pokryciu straty"},
    {"nazwa": "Uchwała lub postanowienie o zatwierdzeniu rocznego sprawozdania finansowego"},
    {"nazwa": "Sprawozdanie z działalności"},
    {"nazwa": "Opinia biegłego rewidenta / Sprawozdanie z badania rocznego sprawozdania finansowego"}
]
```

#### Known Document Types

| nazwa                                                                                   | Description (EN)                              |
|-----------------------------------------------------------------------------------------|-----------------------------------------------|
| Roczne sprawozdanie finansowe                                                           | Annual financial statement                    |
| Uchwała o podziale zysku bądź pokryciu straty                                           | Resolution on profit distribution / loss coverage |
| Uchwała lub postanowienie o zatwierdzeniu rocznego sprawozdania finansowego             | Resolution approving annual financial statement |
| Sprawozdanie z działalności                                                             | Management report / activity report           |
| Opinia biegłego rewidenta / Sprawozdanie z badania rocznego sprawozdania finansowego    | Auditor's opinion / audit report              |

---

### 3. Wyszukiwanie dokumentów (Search / List Documents)

Returns a paginated list of all documents filed by the entity. This is the main discovery endpoint.

**`POST /dokumenty/wyszukiwanie`**

#### Request

```json
{
    "metadaneStronicowania": {
        "numerStrony": 0,
        "rozmiarStrony": 10,
        "metadaneSortowania": [
            {
                "atrybut": "id",
                "kierunek": "MALEJACO"
            }
        ]
    },
    "nrKRS": "IxW7jON1dHOJSvhGjTLouRR0zd0tTAfUHWXl1rApR5Q="
}
```

| Field                                           | Type    | Required | Description                                          |
|-------------------------------------------------|---------|----------|------------------------------------------------------|
| `metadaneStronicowania.numerStrony`             | integer | yes      | Page number (0-indexed)                              |
| `metadaneStronicowania.rozmiarStrony`           | integer | yes      | Page size (e.g. 10, 20)                              |
| `metadaneStronicowania.metadaneSortowania`      | array   | yes      | Sort criteria                                        |
| `metadaneStronicowania.metadaneSortowania[].atrybut`  | string  | yes | Sort field (`"id"`)                                  |
| `metadaneStronicowania.metadaneSortowania[].kierunek` | string  | yes | Sort direction: `"MALEJACO"` (DESC) or `"ROSNACO"` (ASC) |
| `nrKRS`                                         | string  | yes      | **Encoded/hashed KRS number** - see warning below    |

> **⚠️ `nrKRS` is NOT the plain KRS number - it is an AES-encrypted, Base64-encoded token.**
>
> The frontend encrypts the KRS number client-side before sending it.
> The token changes every second (the plaintext includes the current timestamp).
> The algorithm was reverse-engineered from `main-C7XHMT4M.js` (function `encryptNrKrs`).

#### `nrKRS` Encryption Algorithm

The token is generated using **AES-128-CBC** with PKCS7 padding. Both the key and the IV are the same value.

**Step 1 - Build the plaintext:**

```
plaintext = KRS.padStart(10, "0") + formatDate(now, "yyyy-MM-dd-HH-mm-ss")
```

Example (KRS `694720`, timestamp `2026-03-19-14-30-45`):

```
plaintext = "0000694720" + "2026-03-19-14-30-45"
          = "00006947202026-03-19-14-30-45"
```

**Step 2 - Derive key and IV (they are identical):**

```
raw = formatDate(now, "yyyy-MM-dd-HH")   // e.g. "2026-03-19-14"  (13 chars)
key = raw.padStart(16, "1")               // e.g. "1112026-03-19-14" (16 chars)
iv  = key                                 // same value
```

The `padStart(16, "1")` prepends the character `"1"` until the string is 16 bytes long.
The hour-part of the date (`HH`) is in 24h format. The number of `"1"` prefixed depends
on the date string length (typically 3 for dates in the 2020s, e.g. `"111"`).

**Step 3 - Encrypt:**

```
ciphertext = AES-CBC-encrypt(plaintext, key, iv, padding=PKCS7)
token      = Base64(ciphertext)
```

**Python reference implementation:**

```python
from datetime import datetime
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
import base64

def encrypt_nrkrs(krs: str) -> str:
    now = datetime.now()

    # Plaintext: zero-padded KRS + full timestamp
    timestamp_full = now.strftime("%Y-%m-%d-%H-%M-%S")
    plaintext = krs.zfill(10) + timestamp_full

    # Key/IV: hour-precision timestamp, left-padded with "1" to 16 chars
    timestamp_hour = now.strftime("%Y-%m-%d-%H")
    key = timestamp_hour.rjust(16, "1")

    key_bytes = key.encode("utf-8")
    cipher = AES.new(key_bytes, AES.MODE_CBC, iv=key_bytes)
    encrypted = cipher.encrypt(pad(plaintext.encode("utf-8"), AES.block_size))
    return base64.b64encode(encrypted).decode("utf-8")
```

> **Timing note:** The key depends only on the current hour, but the plaintext includes
> minutes and seconds. The server presumably accepts tokens generated within the same
> hour (or a small tolerance window). If requests fail around the hour boundary,
> retry once with a fresh token.

#### Response `200 OK`

```json
{
    "content": [
        {
            "id": "I4YS9y2_bUJIUqJXUQBB1A==",
            "rodzaj": "4",
            "status": "NIEUSUNIETY",
            "statusBezpieczenstwa": null,
            "nazwa": null,
            "okresSprawozdawczyPoczatek": "2024-01-01",
            "okresSprawozdawczyKoniec": "2024-12-31",
            "dataUsunieciaDokumentu": ""
        }
    ],
    "metadaneWynikow": {
        "numerStrony": 0,
        "rozmiarStrony": 10,
        "liczbaStron": 4,
        "calkowitaLiczbaObiektow": 31
    }
}
```

| Field                                   | Type    | Description                                           |
|-----------------------------------------|---------|-------------------------------------------------------|
| `content[].id`                          | string  | Document ID (Base64-encoded) - used in subsequent calls |
| `content[].rodzaj`                      | string  | Document type code (see mapping below)                |
| `content[].status`                      | string  | `"NIEUSUNIETY"` = active, not deleted                 |
| `content[].statusBezpieczenstwa`        | string? | Security status, usually `null`                       |
| `content[].nazwa`                       | string? | Document name, usually `null`                         |
| `content[].okresSprawozdawczyPoczatek`  | string  | Reporting period start (`YYYY-MM-DD`)                 |
| `content[].okresSprawozdawczyKoniec`    | string  | Reporting period end (`YYYY-MM-DD`)                   |
| `content[].dataUsunieciaDokumentu`      | string  | Deletion date - empty string if not deleted           |
| `metadaneWynikow.numerStrony`           | integer | Current page (0-indexed)                              |
| `metadaneWynikow.rozmiarStrony`         | integer | Page size                                             |
| `metadaneWynikow.liczbaStron`           | integer | Total number of pages                                 |
| `metadaneWynikow.calkowitaLiczbaObiektow` | integer | Total number of documents                           |

#### Known `rodzaj` Codes

| Code | Document Type (PL)                                        | Document Type (EN)              |
|------|-----------------------------------------------------------|---------------------------------|
| `3`  | Uchwała o zatwierdzeniu sprawozdania                      | Approval resolution             |
| `4`  | Uchwała o podziale zysku / pokryciu straty                | Profit distribution resolution  |
| `18` | Roczne sprawozdanie finansowe                             | Annual financial statement      |
| `19` | Sprawozdanie z działalności                               | Management/activity report      |
| `20` | Opinia biegłego rewidenta                                 | Auditor's opinion               |

> **Tip:** To get only financial statements, filter `content` where `rodzaj == "18"`.

---

### 4. ID dokumentu i korekt (Document ID & Corrections)

Returns the document ID and any correction chain. Use this to verify if the document has been corrected.

**`GET /dokumenty/{documentId}/id-dokumentu-i-korekt`**

#### URL Parameters

| Parameter    | Type   | Description                              |
|--------------|--------|------------------------------------------|
| `documentId` | string | URL-encoded Base64 document ID from search results |

#### Example

```
GET /dokumenty/ZgsX8Fsncb1PFW07-T4XoQ%3D%3D/id-dokumentu-i-korekt
```

> **Note:** The `==` at the end of Base64 IDs must be URL-encoded as `%3D%3D`.

#### Response `200 OK`

```json
["ZgsX8Fsncb1PFW07-T4XoQ=="]
```

Returns an array of document IDs. If the document was corrected, this array contains multiple entries (original + corrections).

---

### 5. Document Metadata (Full Details)

Returns complete metadata for a specific document.

**`GET /dokumenty/{documentId}`**

#### Example

```
GET /dokumenty/ZgsX8Fsncb1PFW07-T4XoQ%3D%3D
```

#### Response `200 OK`

```json
{
    "rodzajDokumentu": {
        "id": 18,
        "idSlKategoriaDokumentu": null,
        "kodKRS": "SF",
        "kod": "30",
        "nazwa": "Roczne sprawozdanie finansowe",
        "validFrom": "2018-01-10",
        "validTo": null,
        "standardMSR": true,
        "podpisWymagany": true,
        "wyslijDoMf": true,
        "kolejnoscWyswietlania": 1,
        "dopuszczalneRozszerzenia": ".XML, .XAdES, .XML.SIG, .SIG, .p7m",
        "dopuszczalneRozszerzeniaMSR": ".ZIP, .XHTML, .XML, .PDF, .JPG, ..."
    },
    "status": "NIEUSUNIETY",
    "nazwa": "",
    "okresSprawozdawczyPoczatek": "2024-01-01",
    "okresSprawozdawczyKoniec": "2024-12-31",
    "dataUsunieciaDokumentuPrzezSad": null,
    "dataSporzadzenia": "2025-04-28",
    "czyMSR": false,
    "czyKorekta": false,
    "dataDodania": "2025-05-20",
    "identyfikator": "ZgsX8Fsncb1PFW07-T4XoQ==",
    "identyfikatorZgloszenia": "J0UU5iZTgCYOni83Rx2heQ==",
    "identyfikatorZgloszeniaGui": "4099720",
    "nrKRS": "0000694720",
    "formaPrawna": "SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ",
    "nazwaPodmiotu": "B-JWK-MANAGEMENT SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ",
    "wydzialSadu": null,
    "sygnaturaSprawy": null,
    "jezyk": "polski",
    "nazwaPliku": "Bjwk SF za 2024.xml",
    "uuidDokumentuCRDP": "2F07D70A-646C-420F-BA64-8BCF39E8FD82",
    "idDokumentu": "9530163"
}
```

#### Key Fields

| Field                              | Type    | Description                                             |
|------------------------------------|---------|---------------------------------------------------------|
| `rodzajDokumentu.id`              | integer | Document type ID (18 = annual financial statement)      |
| `rodzajDokumentu.kodKRS`          | string  | KRS code (`"SF"` = sprawozdanie finansowe)              |
| `rodzajDokumentu.nazwa`           | string  | Human-readable document type name                       |
| `rodzajDokumentu.standardMSR`     | boolean | Whether IFRS (MSR) standard is supported                |
| `rodzajDokumentu.dopuszczalneRozszerzenia` | string | Allowed file extensions for Polish GAAP (UoR)   |
| `rodzajDokumentu.dopuszczalneRozszerzeniaMSR` | string | Allowed file extensions for IFRS (MSR)       |
| `czyMSR`                          | boolean | `false` = Polish GAAP (UoR), `true` = IFRS (MSR)       |
| `czyKorekta`                      | boolean | `true` if this is a correction/amendment                |
| `dataSporzadzenia`                | string  | Date the document was prepared (`YYYY-MM-DD`)           |
| `dataDodania`                     | string  | Date the document was filed to RDF (`YYYY-MM-DD`)       |
| `nazwaPliku`                      | string  | Original filename - tells you the format (`.xml`, `.zip`, `.xhtml`) |
| `uuidDokumentuCRDP`              | string  | UUID in the Central Repository (CRDP)                   |
| `idDokumentu`                     | string  | Numeric document ID                                     |
| `jezyk`                           | string  | Language of the document                                |
| `okresSprawozdawczyPoczatek`      | string  | Reporting period start                                  |
| `okresSprawozdawczyKoniec`        | string  | Reporting period end                                    |

#### Important: `czyMSR` determines file format

| `czyMSR` | Standard    | Typical format | XML schema                    |
|----------|-------------|----------------|-------------------------------|
| `false`  | Polish GAAP | `.xml`         | MF XSD schemas (structured)   |
| `true`   | IFRS (MSR)  | `.xhtml`/`.zip`| iXBRL / XHTML (less structured) |

> Polish GAAP XML files follow a strict XSD schema published by the Ministry of Finance
> and are much easier to parse programmatically. IFRS/MSR files are typically XHTML
> with inline XBRL tags and require different parsing logic.

---

### 6. Download Document Content

Downloads the actual document file(s) as a ZIP archive.

**`POST /dokumenty/tresc`**

#### Request

```json
["ZgsX8Fsncb1PFW07-T4XoQ=="]
```

The body is a JSON array of document IDs (strings). You can request multiple documents at once.

#### Headers (override for this endpoint)

```python
DOWNLOAD_HEADERS = {
    **HEADERS,
    "Accept": "application/octet-stream",  # Important: binary response
}
```

#### Response `200 OK`

- **Content-Type:** `application/octet-stream`
- **Body:** ZIP file (binary)

The ZIP archive contains the original document file(s) as submitted by the entity (`.xml`, `.xhtml`, `.pdf`, etc.).

#### Handling the response

```python
response = session.post(
    f"{BASE_URL}/dokumenty/tresc",
    json=["ZgsX8Fsncb1PFW07-T4XoQ=="],
    headers=DOWNLOAD_HEADERS,
)

# Save ZIP
with open("sprawozdanie.zip", "wb") as f:
    f.write(response.content)

# Extract XML from ZIP
import zipfile, io
with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
    for name in zf.namelist():
        print(f"File in ZIP: {name}")
        content = zf.read(name)
```

---

## Full Flow - Python Pseudocode

```python
import requests
from datetime import datetime
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
import base64

BASE = "https://rdf-przegladarka.ms.gov.pl/services/rdf/przegladarka-dokumentow-finansowych"
session = requests.Session()
session.headers.update({
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 ...",
    "Referer": "https://rdf-przegladarka.ms.gov.pl/wyszukaj-podmiot",
    "Origin": "https://rdf-przegladarka.ms.gov.pl",
})

def encrypt_nrkrs(krs: str) -> str:
    """Encrypt KRS number for the wyszukiwanie endpoint (AES-128-CBC)."""
    now = datetime.now()
    plaintext = krs.zfill(10) + now.strftime("%Y-%m-%d-%H-%M-%S")
    key = now.strftime("%Y-%m-%d-%H").rjust(16, "1").encode("utf-8")
    cipher = AES.new(key, AES.MODE_CBC, iv=key)
    encrypted = cipher.encrypt(pad(plaintext.encode("utf-8"), AES.block_size))
    return base64.b64encode(encrypted).decode("utf-8")

krs = "0000694720"

# Step 1: Validate entity
resp = session.post(f"{BASE}/podmioty/wyszukiwanie/dane-podstawowe", json={"numerKRS": krs})
entity = resp.json()
assert entity["czyPodmiotZnaleziony"], f"KRS {krs} not found"

# Step 2: Get available document types (optional, informational)
resp = session.post(f"{BASE}/dokumenty/rodzajeDokWyszukiwanie", json={"nrKRS": krs})
doc_types = resp.json()

# Step 3: Search documents
encoded_krs = encrypt_nrkrs(krs)
payload = {
    "metadaneStronicowania": {
        "numerStrony": 0,
        "rozmiarStrony": 100,
        "metadaneSortowania": [{"atrybut": "id", "kierunek": "MALEJACO"}]
    },
    "nrKRS": encoded_krs,
}
resp = session.post(f"{BASE}/dokumenty/wyszukiwanie", json=payload)
documents = resp.json()

# Step 4: Filter for annual financial statements (rodzaj == "18")
financial_statements = [
    doc for doc in documents["content"]
    if doc["rodzaj"] == "18" and doc["status"] == "NIEUSUNIETY"
]

# Step 5: Get metadata for the most recent statement
latest = financial_statements[0]
doc_id = latest["id"]
doc_id_encoded = requests.utils.quote(doc_id, safe="")

resp = session.get(f"{BASE}/dokumenty/{doc_id_encoded}")
metadata = resp.json()

# Step 6: Download the file
session.headers["Accept"] = "application/octet-stream"
resp = session.post(f"{BASE}/dokumenty/tresc", json=[doc_id])
with open("sprawozdanie.zip", "wb") as f:
    f.write(resp.content)
```

---

## Open Questions / TODOs for Developer

### ~~1. Encoded KRS token in `wyszukiwanie`~~ (RESOLVED)

**Resolved** by reverse-engineering `main-C7XHMT4M.js` (function `encryptNrKrs`).
The token is AES-128-CBC encrypted client-side. See the full algorithm in
[Section 3 - nrKRS Encryption Algorithm](#nrkrs-encryption-algorithm).

### 2. NIP-to-KRS Resolution

This API does not accept NIP directly. To go from NIP to KRS:
- Use the official Open API KRS at `https://prs.ms.gov.pl/krs/openApi` (Swagger docs available)
- Or use `https://api-krs.ms.gov.pl/api/krs/OdspisPelny/{nrKRS}` after resolving the NIP

### 3. Rate Limiting

No rate limits were observed during manual testing, but the server may enforce limits for automated/high-volume access. Implement:
- [ ] Exponential backoff on 429/503 responses
- [ ] Polite delays between requests (e.g. 1-2s)
- [ ] Session reuse (cookies)

### 4. Document Type Code Completeness

Only codes `3`, `4`, `18`, `19`, `20` were observed. There may be more for other entity types. Map all codes by cross-referencing with `rodzajeDokWyszukiwanie` results.

### 5. IFRS / MSR Documents

When `czyMSR == true`, the document is in XHTML/iXBRL format inside the ZIP. These require a different XML parsing strategy than Polish GAAP XML files.

---

## Parsing the Financial Statement XML (Polish GAAP)

The XML follows Ministry of Finance XSD schemas. Key elements to extract:

| XSD Element            | Description                   |
|------------------------|-------------------------------|
| `AktywaRazem`         | Total assets                  |
| `PasywaRazem`         | Total liabilities + equity    |
| `KapitalWlasny`       | Equity                        |
| `ZyskNetto`           | Net profit                    |
| `PrzychodyNetto`      | Net revenue                   |
| `KosztyDzialalnosci`  | Operating costs               |
| `WynikFinansowy`      | Financial result              |

XSD schemas are published at: `https://www.gov.pl/web/kas/struktury-e-sprawozdan`

---

## Error Handling

| HTTP Status | Meaning                              | Action                        |
|-------------|--------------------------------------|-------------------------------|
| 200         | Success                              | Parse JSON / save binary      |
| 400         | Bad request (malformed payload)      | Check payload structure       |
| 404         | Document/entity not found            | Verify ID / KRS number        |
| 500         | Server error                         | Retry with backoff            |
| 503         | Service unavailable (maintenance)    | Retry later                   |
