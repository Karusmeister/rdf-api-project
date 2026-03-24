# KRS Open API Reference

Developer reference for the official Polish Ministry of Justice KRS Open API.

Launched March 2022 under the Act of 11 August 2021 on open data and reuse of public sector information.

## Base URL

```
https://api-krs.ms.gov.pl/api/krs
```

Server: Kestrel (.NET). No authentication required (fully open).

## Endpoints

### GET /OdpisAktualny/{krs_number}

Current extract - the entity's state as of the most recent registry update.

```bash
curl 'https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000694720?rejestr=P&format=json'
```

### GET /OdpisPelny/{krs_number}

Full extract - current state plus complete change history (all 32+ wpisy entries, historical addresses, capital changes, shareholder changes, etc.).

```bash
curl 'https://api-krs.ms.gov.pl/api/krs/OdpisPelny/0000694720?rejestr=P&format=json'
```

Typical response sizes: OdpisAktualny ~10 KB, OdpisPelny ~30 KB (for an entity with ~30 entries).

## Query Parameters

| Parameter | Required | Values | Default | Notes |
|-----------|----------|--------|---------|-------|
| `rejestr` | No | `P` (businesses), `S` (associations) | Auto-detected | Omitting works fine; the API resolves the register from the KRS number |
| `format` | No | `json` | `json` | `pdf` returns 400. Only JSON is supported via API |

## KRS Number Format

10-digit zero-padded string: `0000694720`. Leading zeros are required.

## Response Structure

All responses are wrapped in a single `odpis` root object.

### Envelope

```json
{
  "odpis": {
    "rodzaj": "Aktualny",
    "naglowekA": { ... },
    "dane": {
      "dzial1": { ... },
      "dzial2": { ... },
      "dzial3": { ... },
      "dzial4": { ... },
      "dzial5": { ... },
      "dzial6": { ... }
    }
  }
}
```

OdpisPelny adds a `naglowekP` object containing the full `wpisy` (entries) array.

### naglowekA (header)

| Field | Type | Example | Description |
|-------|------|---------|-------------|
| `rejestr` | string | `"RejP"` | `RejP` = businesses, `RejS` = associations |
| `numerKRS` | string | `"0000694720"` | 10-digit KRS number |
| `dataCzasOdpisu` | string | `"24.03.2026 21:16:34"` | Timestamp of this extract (DD.MM.YYYY HH:MM:SS) |
| `stanZDnia` | string | `"03.12.2025"` | Data freshness date (DD.MM.YYYY). May lag days/weeks behind real time |
| `dataRejestracjiWKRS` | string | `"19.09.2017"` | First registration date |
| `numerOstatniegoWpisu` | number | `32` | Sequence number of the latest entry |
| `dataOstatniegoWpisu` | string | `"20.05.2025"` | Date of latest entry |
| `sygnaturaAktSprawyDotyczacejOstatniegoWpisu` | string | `"RDF/720951/25/730"` | Case reference of latest entry |
| `oznaczenieSaduDokonujacegoOstatniegoWpisu` | string | `"SYSTEM"` | Court or system that made the latest entry |
| `stanPozycji` | number | `1` | Position status |

### dane.dzial1 - Entity data

Structure varies by register type.

**Businesses (RejP):**

| Section | Key fields |
|---------|-----------|
| `danePodmiotu` | `formaPrawna`, `nazwa`, `identyfikatory.regon`, `identyfikatory.nip`, `czyProwadziDzialalnoscZInnymiPodmiotami`, `czyPosiadaStatusOPP` |
| `siedzibaIAdres` | `siedziba.{kraj, wojewodztwo, powiat, gmina, miejscowosc}`, `adres.{ulica, nrDomu, nrLokalu, miejscowosc, kodPocztowy, poczta, kraj}` |
| `umowaStatut` | `informacjaOZawarciuZmianieUmowyStatutu[]` - array of notarial statute changes |
| `pozostaleInformacje` | `czasNaJakiUtworzonyZostalPodmiot` (duration), `informacjaOLiczbieUdzialow` |
| `sposobPowstaniaPodmiotu` | `okolicznosciPowstania`, `opisSposobuPowstaniaInformacjaOUchwale`, `podmioty[]` |
| `wspolnicySpzoo` | Array of shareholders, each with `nazwa`, `identyfikator.regon`, `krs.krs`, `posiadaneUdzialy`, `czyPosiadaCaloscUdzialow` |
| `kapital` | `wysokoscKapitaluZakladowego.{wartosc, waluta}`, `wniesioneAporty` |

**Associations (RejS):**

| Section | Key fields |
|---------|-----------|
| `danePodmiotu` | Same structure as businesses |
| `siedzibaIAdres` | Same structure |
| `umowaStatut` | Same structure |
| `pozostaleInformacje` | Same structure |
| `komitetZalozycielski` | Founding committee information |
| `organSprawujacyNadzor` | Supervisory body information |

### dane.dzial2 - Representation / Management

**Businesses:**

| Section | Key fields |
|---------|-----------|
| `reprezentacja` | `nazwaOrganu` ("ZARZAD"), `sposobReprezentacji`, `sklad[]` |

Each board member in `sklad`:
- `nazwisko.nazwiskoICzlon` - Masked: `"B********"`
- `imiona.imie` - Masked: `"A***"`
- `identyfikator.pesel` - Masked: `"5**********"`
- `funkcjaWOrganie` - e.g. `"PREZES ZARZADU"`
- `czyZawieszona` - boolean

**Associations:** additionally includes `organNadzoru`.

### dane.dzial3 - Activities & financial filings

**Businesses:**

| Section | Key fields |
|---------|-----------|
| `przedmiotDzialalnosci` | `przedmiotPrzewazajacejDzialalnosci.{opis, kodDzial, kodKlasa, kodPodklasa}`, `przedmiotPozostalejDzialalnosci[]` |
| `wzmiankiOZlozonychDokumentach` | `wzmiankaOZlozeniuRocznegoSprawozdaniaFinansowego[]`, `wzmiankaOZlozeniuOpiniiBieglegoRewidentaSprawozdaniaZBadania[]`, `wzmiankaOZlozeniuUchwalyPostanowieniaOZatwierdzeniuRocznegoSprawozdaniaFinansowego[]`, `wzmiankaOZlozeniuSprawozdaniaZDzialalnosci[]` |
| `informacjaODniuKonczacymRokObrotowy` | `dzienKonczacyPierwszyRokObrotowy` |

Each filing note includes the filing date and the period covered.

**Associations:** includes `celDzialaniaOrganizacji` instead of business activities.

### dane.dzial4-6

Divisions 4-6 are typically empty for active entities:
- **dzial4**: Arrears, pledges, mortgages
- **dzial5**: Curator appointment information
- **dzial6**: Additional information / dissolution proceedings

## GDPR: Anonymized Personal Data

JSON responses anonymize personal data. PDF responses (available only through the web portal, not API) contain full data.

| Field | Format | Example |
|-------|--------|---------|
| Surname | First letter + asterisks | `"B********"` |
| First name | First letter + asterisks | `"A***"` |
| PESEL | First digit + asterisks | `"5**********"` |

Personal addresses of natural persons are not included at all.

## Error Responses

Errors use RFC 7807 Problem Details format:

### 404 - Entity Not Found

```json
{
  "type": "https://tools.ietf.org/html/rfc7231#section-6.5.4",
  "title": "Not Found",
  "status": 404,
  "traceId": "00-6c1db6e5df3c3fc6ef16179ad09772ef-4ebd164930768a49-00"
}
```

### 400 - Bad Request

Returned for invalid parameters (e.g., `format=pdf`).

```json
{
  "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
  "title": "Bad Request",
  "status": 400,
  "traceId": "00-3fa2d90041a9f012d8cd18e0b5380ee1-4c30034b7502df86-00"
}
```

## Rate Limiting

No documented rate limits. No `429 Too Many Requests` observed during testing with rapid sequential requests. However:

- The API is a government service with no SLA
- Aggressive automated access may trigger IP-level blocking (unconfirmed)
- Recommended: 1-2 second delay between requests for polite pacing
- The existing scraper in this project uses a 2-second delay between KRS entities

## Limitations

1. **No search endpoint.** You must know the KRS number. There is no name, NIP, or REGON search via this API. The search UI at `prs.ms.gov.pl/krs` is a browser-only SPA with no public API.
2. **No batch endpoint.** One entity per request.
3. **No delta/changelog feed.** No way to get "entities changed since date X." The government announcement mentioned "list of entities where entries were made on a given day" but no such endpoint was found on `api-krs.ms.gov.pl`.
4. **JSON only.** `format=pdf` returns 400. PDFs are only available through the web portal.
5. **Data freshness lag.** The `stanZDnia` field shows the registry snapshot date, which can lag behind real-time by days or weeks.
6. **No WebSocket or push notifications.**

## Comparison with Existing RDF Proxy

The existing RDF proxy (`rdf-przegladarka.ms.gov.pl`) in this project serves a different purpose:

| Feature | KRS Open API | RDF Proxy |
|---------|-------------|-----------|
| Base URL | `api-krs.ms.gov.pl` | `rdf-przegladarka.ms.gov.pl` |
| Data | Entity registration data (officers, capital, shareholders, address) | Financial documents (e-Sprawozdania XML/ZIP) |
| Auth | None | KRS encryption token for search |
| Overlap | `identyfikatory.nip`, `identyfikatory.regon`, `nazwa`, `formaPrawna` | Same identifiers available in `dane-podstawowe` |
| Unique to | Full corporate history, shareholders, capital, board members, business activities, filing dates | Actual financial statement files (balance sheet, P&L, cash flow) |

For the Bankruptcy Prediction Engine, both APIs are needed:
- KRS Open API: entity metadata, legal form, capital, board changes, filing history
- RDF Proxy: actual financial numbers for ratio computation

## Example: Full Request/Response

```bash
curl -s 'https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000694720?rejestr=P&format=json' \
  -H 'Accept: application/json'
```

Response (abbreviated):

```json
{
  "odpis": {
    "rodzaj": "Aktualny",
    "naglowekA": {
      "rejestr": "RejP",
      "numerKRS": "0000694720",
      "dataCzasOdpisu": "24.03.2026 21:16:34",
      "stanZDnia": "03.12.2025",
      "dataRejestracjiWKRS": "19.09.2017",
      "numerOstatniegoWpisu": 32,
      "dataOstatniegoWpisu": "20.05.2025"
    },
    "dane": {
      "dzial1": {
        "danePodmiotu": {
          "formaPrawna": "SPOLKA Z OGRANICZONA ODPOWIEDZIALNOSCIA",
          "nazwa": "B-JWK-MANAGEMENT SPOLKA Z OGRANICZONA ODPOWIEDZIALNOSCIA",
          "identyfikatory": {
            "regon": "22204956600000",
            "nip": "5842734981"
          }
        },
        "siedzibaIAdres": {
          "siedziba": {
            "kraj": "POLSKA",
            "wojewodztwo": "POMORSKIE",
            "powiat": "GDANSK",
            "gmina": "GDANSK",
            "miejscowosc": "GDANSK"
          },
          "adres": {
            "ulica": "UL. MYSLIWSKA",
            "nrDomu": "116",
            "kodPocztowy": "80-175"
          }
        },
        "kapital": {
          "wysokoscKapitaluZakladowego": {
            "wartosc": "5300,00",
            "waluta": "PLN"
          }
        }
      },
      "dzial2": {
        "reprezentacja": {
          "nazwaOrganu": "ZARZAD",
          "sposobReprezentacji": "DO SKLADANIA OSWIADCZEN... KAZDY CZLONEK ZARZADU SAMODZIELNIE.",
          "sklad": [
            {
              "nazwisko": {"nazwiskoICzlon": "B********"},
              "imiona": {"imie": "A***"},
              "identyfikator": {"pesel": "5**********"},
              "funkcjaWOrganie": "PREZES ZARZADU"
            }
          ]
        }
      },
      "dzial3": {
        "przedmiotDzialalnosci": {
          "przedmiotPrzewazajacejDzialalnosci": [
            {"opis": "KUPNO I SPRZEDAZ NIERUCHOMOSCI NA WLASNY RACHUNEK", "kodDzial": "68", "kodKlasa": "10", "kodPodklasa": "Z"}
          ]
        },
        "wzmiankiOZlozonychDokumentach": {
          "wzmiankaOZlozeniuRocznegoSprawozdaniaFinansowego": ["7 annual filings from 2017-2024"]
        }
      }
    }
  }
}
```
