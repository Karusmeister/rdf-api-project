# KRS Document Types per Entity Type

Analysis based on sample documents downloaded from the RDF repository (3 KRS entities per type, April 2026).

## Document Type Codes (rodzaj)

The RDF repository uses numeric `rodzaj` codes to classify documents. Canonical mapping
validated against live `/dokumenty/rodzajeDokWyszukiwanie` and `/dokumenty/{id}` metadata
(see `docs/RDF_API_DOCUMENTATION.md` for full reference):

| Code | Meaning (Polish) | English | Notes |
|------|-------------------|---------|-------|
| 1 | Informacja dodatkowa / Inne dokumenty z art. 48 | Additional information / Other docs per Art. 48 | |
| 3 | Uchwała / Postanowienie o zatwierdzeniu | Approval resolution | |
| 4 | Uchwała o podziale zysku / pokryciu straty | Profit distribution / loss coverage resolution | |
| **18** | **Sprawozdanie finansowe (SF)** | **Annual financial statement** | `rodzajDokumentu.kodKRS = "SF"`. Use this to filter for parseable XML statements |
| 19 | Sprawozdanie z działalności | Management/activity report | |
| 20 | Opinia biegłego rewidenta / Sprawozdanie z badania | Auditor's opinion / Audit report | |

---

## 1. Spolka z ograniczona odpowiedzialnoscia (Sp. z o.o. / Limited Liability Company)

**Sample:** KRS 0000184333 (97 docs), 0000015490 (135 docs), 0000125817 (127 docs)

### Document Types
- **Financial statements (SF/SprFin)** - Annual balance sheets, income statements, notes
- **Group/consolidated reports (GK)** - Consolidated statements for companies with subsidiaries
- **eSPR electronic reports** - Structured XML regulatory filings
- **Auditor reports** - Independent auditor opinions
- **Resolutions (Uchwaly)** - Shareholder meeting resolutions on approval and profit distribution
- **Management reports** - Board activity reports

### File Formats
- PDF (dominant ~70%), XML (~10%), XAdES digitally signed (~20%)
- Modern filings use eSPR XML format with XAdES digital signatures
- Older filings are primarily PDF

### Period Coverage
- Ranges from 2001-2025 depending on entity age
- Annual filing cycle, some gaps in older records

### Key Patterns
- Largest and most standardized document set across all types
- Dual reporting: XML (electronic) + PDF (formal) versions common
- Consolidated group reports (GK) present for holding companies
- Filing format evolved from PDF-only to structured XML with digital signatures over time

---

## 2. Spolka Komandytowa (Limited Partnership)

**Sample:** KRS 0000045915 (87 docs), 0000173778 (85 docs), 0000044952 (49 docs)

### Document Types
- **Bilans** (Balance Sheet)
- **Rachunek Zyskow i Strat / RZiS** (Income Statement)
- **Informacja Dodatkowa** (Additional Notes)
- **Sprawozdanie Komplementariusza** (Complementary Partner Reports) - unique to this type
- **Protokol** (Meeting Minutes)
- **Uchwaly** (Resolutions)
- **eSPR / JPK_SF** (Electronic regulatory filings)

### File Formats
- Mixed: PDF (older entities), XML/JPK_SF (newer entities), JPG (scanned historical docs)
- Clear format evolution: scanned images (pre-2010) -> PDF (2010s) -> XML (2020s)

### Period Coverage
- Spans 2002-2024 depending on entity
- Annual filing cycle

### Key Patterns
- **Complementary partner reports** (Sprawozdanie Komplementariusza) are distinctive to this partnership structure
- Older entities have heavily scanned/digitized historical documents (JPG format)
- Modern entities use JPK_SF (Standardized Control File) XML format
- Resolution documents formalize partner decisions

---

## 3. Spolka Jawna (General Partnership)

**Sample:** KRS 0000006365 (27 docs), 0000006401 (5 docs), 0000158657 (96 docs)

### Document Types
- **Bilans** (Balance Sheet)
- **Rachunek Zyskow i Strat** (Income Statement)
- **Informacja Dodatkowa** (Additional Notes)
- **Sprawozdanie z badania / Sprawozdanie bieglego** (Auditor Reports)
- **Uchwaly** (Resolutions - approval, profit distribution)
- **Sprawozdanie roczne** (Annual Reports)

### File Formats
- PDF (professional documents), XML (regulatory filings)
- JPG (67 files in one entity - heavily scanned historical documents)
- ODT (15 files - open document format, unusual)
- XAdES (digitally signed)

### Period Coverage
- Ranges from 2003-2024, with extensive historical archives for older entities

### Key Patterns
- Wide variation in document volume (5 to 96 docs) reflecting entity size differences
- Older entities have extensive JPG scanning of paper documents (image-based archiving)
- Smaller partnerships may have minimal documentation (just balance sheet + notes)
- Auditor reports present for larger partnerships

---

## 4. Fundacja (Foundation)

**Sample:** KRS 0000014664 (3 docs), 0000163029 (37 docs), 0000115481 (56 docs)

### Document Types
- **Bilans** (Balance Sheet)
- **Rachunek Zyskow i Strat** (Income Statement)
- **Informacja Dodatkowa** (Additional Information)
- **Uchwaly** (Resolutions / Board Decisions)
- **Protokol Zatwierdzenia** (Approval Protocols)
- **Sprawozdanie** (Comprehensive financial statements)
- **Dissolution documents** - Liquidator resolutions, liquidation period docs (for dissolved entities)

### File Formats
- PDF (dominant for resolutions/governance docs)
- XML / eSPR (electronic financial filings)
- XAdES (digitally signed - 56 docs in one entity were exclusively XAdES)
- JPG (occasional)

### Period Coverage
- Ranges from 2009-2024
- Some entities have 10+ year historical records

### Key Patterns
- Governance documentation is prominent (board decisions, approval protocols)
- Dissolved foundations include liquidator resolutions and dissolution-period documents
- Heavy use of XAdES digital signatures in some entities
- Longer historical records compared to commercial entities

---

## 5. Spoldzielnia (Cooperative)

**Sample:** KRS 0000006811 (43 docs), 0000006969 (6 docs), 0000007056 (12 docs)

### Document Types
- **Sprawozdanie Finansowe (SF)** - Annual financial statements (primary)
- **Sprawozdanie Zarzadu** - Management board reports
- **Uchwaly** - Resolutions (financial approval + profit/surplus distribution)
- **Sprawozdanie bieglego rewidenta / SzB** - Auditor reports
- **eSPR** - Electronic regulatory filings (XML)
- **Bilans, RZiS** - Balance sheet, income statement (components)
- **Zestawienie zmian w kapitalach** - Statement of changes in equity

### File Formats
- PDF (dominant ~83%)
- XML (17% - structured regulatory reports)
- Hybrid files: .pdf.xml, .xml.xml (format conversion artifacts)

### Period Coverage
- 2017-2024 (varies by entity)

### Key Patterns
- Member-focused documents: profit distribution uses "nadwyzka bilansowa" (balance surplus) terminology specific to cooperatives
- Mandatory financial documentation filed annually
- Auditor reports common (cooperatives often meet audit thresholds)
- Statement of changes in equity more frequent than in smaller entity types

---

## 6. Spolka Akcyjna (Joint-Stock Company)

**Sample:** KRS 0000006553 (44 docs), 0000006691 (34 docs), 0000006709 (28 docs)

### Document Types
- **Sprawozdania finansowe** - Financial reports (primary)
- **Sprawozdania Zarzadu** - Management board statements
- **Uchwaly WZA/ZWZ** - Shareholder meeting resolutions (AGM/EGM)
- **Opinie bieglego rewidenta** - Auditor opinions
- **Informacje dodatkowe** - Supplementary information
- **Uchwaly podzial zysku** - Profit distribution resolutions
- **Akty notarialne** - Notarial acts
- **Rachunek przeplywow pienieznych** - Cash flow statements

### File Formats
- PDF (primary ~60%)
- XML (~15%)
- XAdES / XML.XAdES (~25% - digitally signed)

### Period Coverage
- 2018-2025 (consistent multi-year coverage)

### Key Patterns
- **Richest document set** among all types - most complete financial reporting
- **Dual format filing**: signed documents exist in both .pdf + .pdf.xades pairs
- **Governance focus**: heavy emphasis on shareholder resolutions, profit distribution, notarial acts
- **Cash flow statements** more consistently present than in smaller entity types
- **Digital signature compliance**: ~40% of files have XAdES signatures
- **Auditor reports mandatory** for all joint-stock companies

---

## 7. Stowarzyszenie (Association)

**Sample:** KRS 0000006397 (23 docs), 0000007234 (5 docs), 0000007666 (33 docs)

### Document Types
- **Sprawozdania finansowe** - Financial reports (XML format)
- **Bilans** - Balance sheets (PDF)
- **Rachunek zyskow i strat** - Income statements (PDF)
- **Informacja dodatkowa** - Additional information (PDF)
- **Sprawozdania bieglego rewidenta z badania** - Auditor reports
- **Uchwaly** - Assembly resolutions (financial, governance)
- **Protokoly** - Meeting minutes (assembly, scrutiny committees)
- **Zatwierdzenie sprawozdania** - Statement approval documents
- **Podzial zysku/wyniku finansowego** - Profit/result distribution

### File Formats
- PDF (dominant ~67%)
- XML (~23%)
- XAdES (~10% - digitally signed)

### Period Coverage
- 2017-2025

### Key Patterns
- **Governance documents heavily represented** - assembly protocols, committee reports reflect association structure
- **Dual reporting**: parallel XML (electronic) and PDF (formal) versions of financial statements
- **Complete financial packages**: balance sheets, income statements, and auditor opinions bundled together
- **Annual assembly cycle** visible in document clustering

---

## 8. Samodzielny Publiczny Zaklad Opieki Zdrowotnej - SP ZOZ (Public Healthcare Facility)

**Sample:** KRS 0000006384 (4 docs), 0000006396 (17 docs)

### Document Types
- **SprFinJednostkaInnaWZlotych** - Financial statement for "other unit" in PLN (specific to public entities)
- **Uchwaly** - Municipal resolutions / approval resolutions
- **Zatwierdzenie sprawozdania** - Financial statement approvals
- **Sprawozdanie z badania** - Audit reports
- **Sprawozdanie KPW** - Health Insurance Fund (NFZ) reports - unique to healthcare

### File Formats
- XML (financial statements - structured)
- PDF (approvals, audit reports, governance docs)

### Period Coverage
- 2019-2024

### Key Patterns
- **Public entity financial format**: uses "JednostkaInna" (Other Unit) classification, not commercial entity forms
- **Health Insurance Fund reporting** (Sprawozdanie KPW) is unique to this entity type
- **Municipal governance**: approval documents come from municipal authorities, not shareholders
- **Comprehensive multi-year preservation** with audit documentation
- **Dual format**: XML for technical submissions, PDF for formal approvals

---

## 9. Oddzial Zagranicznego Przedsiebiorcy (Foreign Enterprise Branch)

**Sample:** KRS 0000007238 (11 docs), 0000016563 (3 docs), 0000017064 (10 docs)

### Document Types
- **SFJINZ - SprFinJednostkaInnaWZlotych** - Financial statement for "other unit" in PLN (standard for foreign branches)
- **SFJMAZ - SprFinJednostkaMalaWZlotych** - Small unit financial statement in PLN
- **Uchwaly** - Board resolutions / shareholder decisions

### File Formats
- XML (dominant ~79% - structured financial data per Polish Ministry of Finance schema)
- XAdES (~17% - digitally signed XML)
- PDF (rare ~4% - only for resolution documents)

### Period Coverage
- Varies widely: 2010-2026 (British Airways), 2010+2021 (BESIX, sparse), 2017-2024 (Finnair)

### Key Patterns
- **All branches report in PLN** using Polish standardized forms despite being foreign entities
- **XML-dominant**: foreign branches prefer structured XML filings over PDF
- **Digital signatures** (XAdES) present across all entities for regulatory compliance
- **Sparse filing** for some entities suggests inconsistent compliance or data collection gaps
- Uses "JednostkaInna" (Other Unit) or "JednostkaMala" (Small Unit) forms, not standard commercial forms

---

## 10. Stowarzyszenie Kultury Fizycznej (Sports Culture Association)

**Sample:** KRS 0000008421 (2 docs), 0000010431 (1 doc), 0000013491 (15 docs)

### Document Types
- **Uchwaly** - Assembly resolutions and financial approvals (most common)
- **Sprawozdania finansowe** - Annual financial statements (XML)
- **eSPR** - Electronic sports/financial reports
- **Protokoly** - General assembly meeting proceedings

### File Formats
- PDF (dominant ~77% - governance and resolutions)
- XML (~23% - financial statements and eSPR regulatory reports)

### Period Coverage
- 2020-2024

### Key Patterns
- **Governance-heavy**: assembly resolutions dominate over financial statements
- **Smaller document volumes** than commercial entities (2-15 docs vs 30-130+)
- **Dual filing**: resolutions in PDF for readability + XML financial reports for regulatory submission
- **Annual assembly cycle** clearly visible in document clustering
- Some entities have very minimal documentation (1-2 documents only)

---

## Cross-Type Comparison

### Document Volume (avg docs per entity)

| Entity Type | Avg Docs | Min | Max |
|-------------|----------|-----|-----|
| Sp. z o.o. | 120 | 97 | 135 |
| Sp. komandytowa | 74 | 49 | 87 |
| Sp. jawna | 43 | 5 | 96 |
| Sp. akcyjna | 35 | 28 | 44 |
| Fundacja | 32 | 3 | 56 |
| Stowarzyszenie | 20 | 5 | 33 |
| SP ZOZ | 11 | 4 | 17 |
| Spoldzielnia | 20 | 6 | 43 |
| Oddz. zagr. | 8 | 3 | 11 |
| Stow. kult. fiz. | 6 | 1 | 15 |

### File Format Distribution

| Entity Type | PDF | XML | XAdES | JPG/Other |
|-------------|-----|-----|-------|-----------|
| Sp. z o.o. | High | Medium | High | Rare |
| Sp. komandytowa | Medium | Medium | Low | High (scans) |
| Sp. jawna | Medium | Low | Low | High (JPG, ODT) |
| Sp. akcyjna | High | Medium | High | Rare |
| Fundacja | Medium | Medium | High | Rare |
| Spoldzielnia | High | Low | Rare | Rare |
| Stowarzyszenie | High | Medium | Low | Rare |
| SP ZOZ | Medium | Medium | Rare | Rare |
| Oddz. zagr. | Low | High | Medium | Rare |
| Stow. kult. fiz. | High | Low | Rare | Rare |

### Unique Document Types per Entity Type

| Entity Type | Unique Documents |
|-------------|-----------------|
| Sp. komandytowa | Sprawozdanie Komplementariusza (Complementary Partner Reports) |
| Sp. akcyjna | Akty notarialne (Notarial Acts), Rachunek przeplywow pienieznych (Cash Flow) |
| SP ZOZ | Sprawozdanie KPW (Health Insurance Fund Reports), Municipal resolutions |
| Oddz. zagr. | SprFinJednostkaMala (Small Unit form), PLN-denominated foreign reports |
| Spoldzielnia | Nadwyzka bilansowa (Balance Surplus distribution) |
| Fundacja | Dissolution/liquidation documents |

### Filing Format Evolution

All entity types show a clear evolution:
1. **Pre-2010**: Scanned paper documents (JPG, TIFF)
2. **2010-2018**: PDF dominates (both scanned and born-digital)
3. **2018-present**: Structured XML (eSPR, JPK_SF) with XAdES digital signatures becomes standard
4. **Public entities** (SP ZOZ, foreign branches) adopted XML earlier than private entities

### Implications for the Prediction Pipeline

1. **XML coverage**: Only XML documents (eSPR format) can be parsed by the `xml_parser.py` pipeline. PDF-only entities require OCR or manual processing.
2. **Type-specific parsers**: SP ZOZ uses "JednostkaInna" forms with different XML structure than standard commercial forms.
3. **Historical gaps**: Older entities may have JPG-only records that cannot be automatically ingested.
4. **Cooperative-specific**: Cooperatives use surplus distribution terminology that differs from standard profit/loss.
5. **Foreign branches**: Use non-standard financial statement forms (SFJINZ/SFJMAZ) that may need separate parser paths.

---

# Part 2: Deep XML Schema Analysis

## Legal Basis

Article 45(1g) of the **Ustawa o rachunkowosci** (Accounting Act of 29 September 1994) mandates that entities registered in the KRS business register must prepare financial statements in an XML logical structure published by the Minister of Finance. This obligation took effect **1 October 2018**.

The system is colloquially called **JPK_SF** (Jednolity Plik Kontrolny -- Sprawozdanie Finansowe), though technically JPK_SF is the e-filing wrapper; the schemas themselves are called **struktury logiczne e-sprawozdan finansowych**.

**Important**: IFRS-based statements (MSR/MSSF) do NOT have published logical structures. Listed companies reporting under IFRS submit in ESEF/XBRL format via KNF, not through the MF e-Sprawozdania system.

## Official Sources

| Source | URL |
|--------|-----|
| XSD schema downloads (KAS) | https://www.gov.pl/web/kas/struktury-e-sprawozdan |
| e-Sprawozdania info portal | https://www.podatki.gov.pl/e-sprawozdania-finansowe/ |
| Ministry of Finance schemas | https://www.gov.pl/web/finanse/struktury-e-sprawozdan |
| Archive (pre-2020) | https://mf-arch2.mf.gov.pl/web/bip/ministerstwo-finansow/dzialalnosc/rachunkowosc/struktury-e-sprawozdan |

## Complete Schema Type Catalog

Each schema corresponds to an Annex of the Accounting Act. Each entity type has **two currency variants**: `WZlotych` (amounts in PLN) and `WTysiacach` (amounts in thousands of PLN). The root XML element follows the pattern `SprFin{EntityType}W{Currency}`.

For each entity type, there are typically **three XSD files**: `*StrukturyDanychSprFin` (structure definitions), `*WZlotych` (PLN), and `*WTysiacach` (thousands PLN), plus shared `DefinicjeTypySprawozdaniaFinansowe` common types.

| # | kodSystemowy | Root Element (PLN) | Polish Name | Accounting Act Annex | Who Must Use It |
|---|---|---|---|---|---|
| 1 | **SFJINZ** | `SprFinJednostkaInnaWZlotych` | Jednostka inna | Annex 1 | Standard commercial entities: sp. z o.o., S.A., sp.k., sp.j., spoldzielnia, SP ZOZ, etc. -- the most common type |
| 2 | **SFJMAZ** | `SprFinJednostkaMalaWZlotych` | Jednostka mala | Annex 5 | Small entities meeting Art. 3 sec. 1c size criteria (simplified reporting) |
| 3 | **SFJMIZ** | `SprFinJednostkaMikroWZlotych` | Jednostka mikro | Annex 4 | Micro entities meeting Art. 3 sec. 1a criteria (most simplified) |
| 4 | **SFJOPZ** | `SprFinJednostkaOPPWZlotych` | Organizacja pozytku publicznego | Annex 6 | Public benefit organizations (OPP): fundacje, some stowarzyszenia |
| 5 | SFSINZ | `SprFinSkonsolidowanaJednostkaInnaWZlotych` | Skonsolidowana jednostka inna | Annex 1 (consolidated) | Parent companies filing consolidated group statements |
| 6 | SFBWZ | `SprFinBankWZlotych` | Bank | Annex 2 | Banks |
| 7 | **SFZURT** | `SprFinZakladUbezpieczenWTysiacach` | Zaklad ubezpieczen/reasekuracji | Annex 3 | Insurance and reinsurance companies |
| 8 | SFDMWZ | `SprFinDomMaklerskiWZlotych` | Dom maklerski | Ministerial regulation | Brokerage houses |
| 9 | - | `SprFinSKOKWZlotych` | SKOK | Ministerial regulation | Cooperative credit unions |
| 10 | - | `SprFinASIWZlotych` | Alternatywna spolka inwestycyjna | Ministerial regulation | Alternative investment funds |
| 11-13 | - | `SprFinFunduszInwestycyjny*` | Fundusze inwestycyjne | Ministerial regulation | Investment funds (individual, combined, with subfunds) |

## Critical Insight: Schema Is NOT Determined by Legal Form

**The XML schema used is NOT determined by the entity's legal form (sp. z o.o., S.A., etc.). It is determined by the size category the entity self-selects under the Accounting Act.**

A sp. z o.o. can file as JednostkaMikro, JednostkaMala, or JednostkaInna depending on whether it qualifies as micro, small, or "other" (full). The only exceptions are special-purpose schemas:
- **JednostkaOp** for non-profit organizations (fundacje, some stowarzyszenia filing as OPP)
- **ZakladUbezpieczen** for insurance companies
- **Bank** for banks
- Etc.

### Evidence from Our Samples

| Entity Type | KRS | XML Schema Actually Used |
|---|---|---|
| Sp. z o.o. | 0000184333 | **JednostkaMikro** (SFJMIZ) |
| Sp. z o.o. | 0000015490 | **JednostkaInna** (SFJINZ) |
| Sp. komandytowa | 0000045915 | **JednostkaMikro** (SFJMIZ) |
| Sp. komandytowa | 0000173778 | **JednostkaMikro** (SFJMIZ) |
| Sp. jawna | 0000158657 | **JednostkaMala** (SFJMAZ) |
| Sp. jawna | 0000006365 | **JednostkaInna** (SFJINZ) |
| Fundacja | 0000163029 | **JednostkaOp** (SFJOPZ) |
| Sp. akcyjna | 0000006553 | **JednostkaInna** (SFJINZ) |
| Sp. akcyjna (ubezpieczenia) | 0000006691 | **ZakladUbezpieczen** (SFZURT) |
| Spoldzielnia | 0000006811 | **JednostkaInna** (SFJINZ) |
| Stowarzyszenie | 0000007666 | **JednostkaInna** (SFJINZ) |
| SP ZOZ | 0000006384, 0000006396 | **JednostkaInna** (SFJINZ) |
| Oddz. zagraniczny | 0000007238 | **JednostkaInna** (SFJINZ) |
| Oddz. zagraniczny | 0000017064 (Finnair) | **JednostkaMala** (SFJMAZ) |
| Stow. kult. fiz. | 0000013491 | **JednostkaInna** (SFJINZ) |
| Stow. kult. fiz. | 0000008421 | **JednostkaMikro** (SFJMIZ) |

## Sections Present in Each Schema

| Section | JednostkaInna (SFJINZ) | JednostkaMala (SFJMAZ) | JednostkaMikro (SFJMIZ) | JednostkaOp (SFJOPZ) | ZakladUbezp (SFZURT) |
|---|---|---|---|---|---|
| **Naglowek** (Header) | Yes | Yes | Yes | Yes | Yes |
| **Wprowadzenie** (Introduction) | WprowadzenieDoSprawozdaniaFinansowego | ...JednostkaMala | InformacjeOgolneJednostkaMikro | ...JednostkaOp | Wstep |
| **Bilans** (Balance Sheet) | Bilans | BilansJednostkaMala | BilansJednostkaMikro | BilansJednostkaOp | BilansZakladUbezpieczen |
| **RZiS** (Income Statement) | RZiS | RZiSJednostkaMala | RZiSJednostkaMikro | RZiSJednostkaOp | RZiSZakladUbezpieczen |
| **ZestZmianWKapitale** (Equity Changes) | Optional | No | No | No | Yes |
| **RachPrzeplywow** (Cash Flow) | Optional | No | No | No | Yes |
| **Dodatkowe Informacje** (Notes) | DodatkoweInformacjeIObjasnieniaJednostkaInna | ...JednostkaMala | InformacjeUzupelniajaceDoBilansu | InformacjaDodatkowaJednostkaOp | ...ZakladUbezpieczen |
| **Pozabilansowe** (Off-balance) | No | No | No | No | Yes |
| **TRUMajatek** (Technical reserves) | No | No | No | No | Yes |

## Bilans (Balance Sheet) Depth Comparison

| Schema | Depth | Example Tags | Notes |
|---|---|---|---|
| **JednostkaInna** | Full (3-4 levels) | `Aktywa_A_I_1`, `Aktywa_A_I_2`, `Aktywa_B_I_1_a` | Most granular, ~100+ line items |
| **JednostkaMala** | Intermediate (2 levels) | `Aktywa_A`, `Aktywa_B`, `Aktywa_D` | ~30-40 line items |
| **JednostkaMikro** | Shallow (1-2 levels) | `Aktywa_A`, `Aktywa_B`, `Aktywa_B_1`, `Aktywa_B_2`, `Aktywa_C`, `Aktywa_D` | ~10-15 line items only |
| **JednostkaOp** | Intermediate (2-3 levels) | `Aktywa_A_I` through `Aktywa_A_V`, `Aktywa_B_I` through `Aktywa_B_IV` | Similar to JednostkaInna but with nonprofit-specific labels |
| **ZakladUbezpieczen** | Deep but different structure | `Aktywa_A_1`, `Aktywa_B_I`, `Aktywa_B_II`, `Aktywa_B_III` | Insurance-specific: WNiP, Lokaty (investments), etc. |

Each Bilans element contains `KwotaA` (current period), `KwotaB` (prior period), and optionally `KwotaB1` (restated prior).

## RZiS (Income Statement) Variants

### JednostkaInna and JednostkaMala: Two Variants

The entity chooses one:
- **RZiSPor** (wariant porownawczy / comparative method) -- groups costs by nature (materials, services, salaries, depreciation). Top-level children: A through N.
- **RZiSKalk** (wariant kalkulacyjny / cost-of-sales method) -- groups costs by function (COGS, selling, admin). Top-level children: A through N (different breakdown).

### JednostkaMikro: Single Simplified Format

No porownawczy/kalkulacyjny distinction. Single abbreviated RZiS with elements A through F only.

### JednostkaOp: Non-Profit Specific Format

Own unique RZiS with letters A through O, reflecting non-profit structure:
- A: Przychody z dzialalnosci statutowej (Revenue from statutory activities)
- B: Koszty dzialalnosci statutowej (Costs of statutory activities)
- C-O: Other non-profit specific items

No por/kalk distinction.

### ZakladUbezpieczen: Insurance-Specific Format

Completely different structure with Roman numeral sections (I, II, III, V, VI...) covering:
- Techniczny rachunek ubezpieczen (Technical insurance account)
- Ogolny rachunek zyskow i strat (General profit and loss account)

## Schema Version History

| Version | Effective From | Applies To | Key Changes |
|---|---|---|---|
| **v1-0** | 1 Oct 2018 | All entity types | Initial release |
| **v1-1** | Early 2019 | All | Minor corrections |
| **v1-2** | 1 Sep 2019 | All (major update) | Added ASI, MalaSKOK, SkonsolidowanyDomMaklarski, SkonsolidowanyZakladUbezpieczen |
| **v1-3** | Fiscal years from 1 Jan 2024 | JednostkaInna/Mala/Mikro/Op/Skonsolidowana | Major update. New nodes: OpisDokumentu, DaneDokumentu for CRWD. Published Nov 2024. **Mandatory for statements from 1 Jan 2025.** |
| **v1-5** | 1 Nov 2020 | Banks, investment funds | Sector-specific updates |
| **v1-6** | 1 Jan 2026 | Investment funds only | Fund schema updates |

Note: versions are NOT sequential across all types -- different entity groups have independent version tracks (e.g., banks jumped to v1-5 while JednostkaInna went to v1-3).

### Namespace Versions

Two namespace date prefixes exist in the wild:
- `2018/07/09` -- original namespace (v1-0 through v1-2)
- `2025/01/01` -- updated namespace (v1-3 only, for JednostkaInna)

The structural namespace for types (`DefinicjeTypySprawozdaniaFinansowe`) remains at `2018/07/09` across all schemas.

### Transition Period

The RDF system and e-Sprawozdania application support both old and new structures until **30 June 2026**. There was a known issue in late 2025/early 2026 where the RDF system initially rejected v1-3 schema files with "Blad! Nie znaleziono schemy XSD" -- this was resolved with a system update.

## Summary: What the Parser Must Handle

The parser dispatches on `kodSystemowy` from the XML header, not on entity type:

```
kodSystemowy -> Schema -> Sections Available
SFJINZ       -> JednostkaInna       -> Bilans (full), RZiS (Por OR Kalk), CF (optional), Equity (optional), Notes
SFJMAZ       -> JednostkaMala       -> BilansJednostkaMala, RZiSJednostkaMala (Por OR Kalk), Notes
SFJMIZ       -> JednostkaMikro      -> BilansJednostkaMikro, RZiSJednostkaMikro, InformacjeUzupelniajace
SFJOPZ       -> JednostkaOp         -> BilansJednostkaOp, RZiSJednostkaOp, InformacjaDodatkowaJednostkaOp
SFZURT       -> ZakladUbezpieczen   -> BilansZakladUbezpieczen, RZiSZakladUbezpieczen, CF, Equity, Off-balance
SFSINZ       -> Skonsolidowana      -> (consolidated variants of JednostkaInna sections)
SFBWZ        -> Bank                -> (bank-specific bilans/RZiS)
```

### Implications for `xml_parser.py`

1. **Current state**: The ~1,300 TAG_LABELS primarily handle **SFJINZ** (JednostkaInna). This covers the majority of filings but misses micro, small, OPP, and insurance entities.
2. **5 schemas, not 10 types**: The parser needs to handle 5 distinct schema families (SFJINZ, SFJMAZ, SFJMIZ, SFJOPZ, SFZURT), not one per legal form.
3. **RZiS dispatch**: Within SFJINZ and SFJMAZ, the parser must detect `RZiSPor` vs `RZiSKalk` and branch accordingly.
4. **Version awareness**: Documents may use v1-0, v1-2, or v1-3 namespaces. Tag names are mostly stable across versions, but v1-3 adds new envelope elements.
5. **Coverage priority**: SFJINZ + SFJMAZ + SFJMIZ covers >95% of entities. SFJOPZ adds non-profits. SFZURT is niche (insurance).
6. **Bilans depth varies**: Feature extraction from JednostkaMikro yields far fewer line items than JednostkaInna. The prediction pipeline must handle sparse features for micro entities.
