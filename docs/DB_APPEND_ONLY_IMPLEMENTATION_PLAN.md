# DB Implementation Plan: Append-Only + Current Views

**Date:** 2026-03-29  
**Status:** Ready for implementation  
**Owner:** Implementation agent

---

## 1. Cel biznesowy

Chcemy spełnić dwa wymagania jednocześnie:

1. **Nie nadpisywać danych historycznych** (append-only).
2. **Mieć prosty odczyt danych aktualnych** (widoki `*_current` / `latest_*`).

Dotyczy to szczególnie:

- zmian encji KRS (np. zmiana nazwy firmy),
- zmian metadanych dokumentu RDF (np. korekty, zmiana statusu, uzupełnienie metadanych),
- wersjonowania danych ETL/feature (już częściowo spełnione).

---

## 2. Decyzje architektoniczne (zamrożone)

1. **Brak planów integracji GUS/GPW w horyzoncie 3-6 miesięcy.**
2. Usuwamy/wyłączamy spekulatywne elementy multi-source:
   - `company_identifiers`,
   - `data_sources` (lub minimum: brak zależności runtime; patrz sekcja 6).
3. **Źródłem prawdy stają się tabele wersji** (historyczne), a tabele dotychczasowe mogą działać przejściowo jako cache/projekcja.
4. Odczyty aplikacyjne mają docelowo korzystać z widoków aktualnych.

---

## 3. Co dziś nie spełnia celu append-only

### 3.1 Encje KRS

- `krs_entities` jest nadpisywane przez `ON CONFLICT DO UPDATE`.
- `krs_registry` też jest nadpisywane dla danych podmiotu.
- Efekt: brak pełnej historii zmian encji.

### 3.2 Dokumenty RDF

- `krs_documents` ma 1 rekord na `document_id`.
- `update_document_metadata`, `mark_downloaded`, `update_document_error` nadpisują ten rekord.
- Efekt: brak historii zmian metadanych dokumentu.

### 3.3 ETL

- Przy `no_xml_found` / `parse_error` tworzone są rekordy `financial_reports` z sentinelami (`fiscal_year=0`, `1970-01-01`), które zanieczyszczają warstwę analityczną.

---

## 4. Inwarianty docelowe (must-have)

1. Każda zmiana encji KRS => nowy rekord wersji.
2. Każda istotna zmiana dokumentu RDF => nowy rekord wersji.
3. Brak fizycznego kasowania historii.
4. Odczyt "co jest aktualne" realizowany przez widoki.
5. `financial_reports` i downstream używają tylko sensownych rekordów biznesowych.

---

## 5. Docelowy model danych

## 5.1 Encje KRS (append-only)

Dodaj tabelę historyczną:

```sql
CREATE SEQUENCE IF NOT EXISTS seq_krs_entity_versions START 1;

CREATE TABLE IF NOT EXISTS krs_entity_versions (
    version_id           BIGINT PRIMARY KEY DEFAULT nextval('seq_krs_entity_versions'),
    krs                  VARCHAR(10) NOT NULL,
    name                 VARCHAR NOT NULL,
    legal_form           VARCHAR,
    status               VARCHAR,
    registered_at        DATE,
    last_changed_at      DATE,
    nip                  VARCHAR(13),
    regon                VARCHAR(14),
    address_city         VARCHAR,
    address_street       VARCHAR,
    address_postal_code  VARCHAR,
    raw                  JSON,
    source               VARCHAR NOT NULL,

    valid_from           TIMESTAMP NOT NULL,
    valid_to             TIMESTAMP,
    is_current           BOOLEAN NOT NULL DEFAULT true,
    snapshot_hash        VARCHAR NOT NULL,
    change_reason        VARCHAR,
    observed_at          TIMESTAMP NOT NULL DEFAULT current_timestamp
);

CREATE INDEX IF NOT EXISTS idx_krs_entity_versions_krs ON krs_entity_versions(krs);
CREATE INDEX IF NOT EXISTS idx_krs_entity_versions_current ON krs_entity_versions(krs, is_current);
CREATE INDEX IF NOT EXISTS idx_krs_entity_versions_valid_from ON krs_entity_versions(valid_from);
```

Widok aktualny:

```sql
CREATE OR REPLACE VIEW krs_entities_current AS
SELECT
    krs, name, legal_form, status, registered_at, last_changed_at,
    nip, regon, address_city, address_street, address_postal_code,
    raw, source, valid_from AS synced_at
FROM (
    SELECT
        kev.*,
        row_number() OVER (
            PARTITION BY kev.krs
            ORDER BY kev.valid_from DESC, kev.version_id DESC
        ) AS rn
    FROM krs_entity_versions kev
    WHERE kev.is_current = true
) ranked
WHERE rn = 1;
```

Algorytm zapisu:

1. Zbuduj snapshot encji i `snapshot_hash` (stabilny hash po kanonicznym JSON).
2. Pobierz aktualną wersję dla `krs`.
3. Jeśli hash taki sam: **nie dodawaj nowej wersji** (opcjonalnie tylko `observed_at`).
4. Jeśli hash inny:
   - zamknij poprzednią wersję (`valid_to = now`, `is_current = false`),
   - dodaj nową wersję (`valid_from = now`, `is_current = true`).

## 5.2 Dokumenty RDF (append-only)

Dodaj tabelę historyczną:

```sql
CREATE SEQUENCE IF NOT EXISTS seq_krs_document_versions START 1;

CREATE TABLE IF NOT EXISTS krs_document_versions (
    version_id           BIGINT PRIMARY KEY DEFAULT nextval('seq_krs_document_versions'),
    document_id          VARCHAR NOT NULL,
    version_no           INTEGER NOT NULL,
    krs                  VARCHAR(10) NOT NULL,

    rodzaj               VARCHAR NOT NULL,
    status               VARCHAR NOT NULL,
    nazwa                VARCHAR,
    okres_start          VARCHAR,
    okres_end            VARCHAR,

    filename             VARCHAR,
    is_ifrs              BOOLEAN,
    is_correction        BOOLEAN,
    date_filed           VARCHAR,
    date_prepared        VARCHAR,

    is_downloaded        BOOLEAN,
    downloaded_at        TIMESTAMP,
    storage_path         VARCHAR,
    storage_backend      VARCHAR,
    file_size_bytes      BIGINT,
    zip_size_bytes       BIGINT,
    file_count           INTEGER,
    file_types           VARCHAR,
    discovered_at        TIMESTAMP,
    metadata_fetched_at  TIMESTAMP,
    download_error       VARCHAR,

    valid_from           TIMESTAMP NOT NULL,
    valid_to             TIMESTAMP,
    is_current           BOOLEAN NOT NULL DEFAULT true,
    snapshot_hash        VARCHAR NOT NULL,
    change_reason        VARCHAR,
    run_id               VARCHAR,
    observed_at          TIMESTAMP NOT NULL DEFAULT current_timestamp,

    UNIQUE(document_id, version_no)
);

CREATE INDEX IF NOT EXISTS idx_krs_doc_versions_doc_current ON krs_document_versions(document_id, is_current);
CREATE INDEX IF NOT EXISTS idx_krs_doc_versions_krs_current ON krs_document_versions(krs, is_current);
CREATE INDEX IF NOT EXISTS idx_krs_doc_versions_valid_from ON krs_document_versions(valid_from);
```

Widok aktualny:

```sql
CREATE OR REPLACE VIEW krs_documents_current AS
SELECT
    document_id, krs, rodzaj, status, nazwa, okres_start, okres_end,
    filename, is_ifrs, is_correction, date_filed, date_prepared,
    is_downloaded, downloaded_at, storage_path, storage_backend,
    file_size_bytes, zip_size_bytes, file_count, file_types,
    discovered_at, metadata_fetched_at, download_error
FROM (
    SELECT
        kdv.*,
        row_number() OVER (
            PARTITION BY kdv.document_id
            ORDER BY kdv.version_no DESC, kdv.version_id DESC
        ) AS rn
    FROM krs_document_versions kdv
    WHERE kdv.is_current = true
) ranked
WHERE rn = 1;
```

Algorytm zapisu:

1. Pobierz aktualny snapshot dokumentu.
2. Zmerguj go z nową zmianą (`patch`).
3. Policz `snapshot_hash`.
4. Jeśli bez zmian => no-op.
5. Jeśli zmiana:
   - zamknij poprzednią wersję (`valid_to`, `is_current=false`),
   - insert nowej wersji z `version_no = max(version_no)+1`.

## 5.3 ETL: próby + widoki "usable"

Dodaj tabelę prób ETL:

```sql
CREATE SEQUENCE IF NOT EXISTS seq_etl_attempts START 1;

CREATE TABLE IF NOT EXISTS etl_attempts (
    attempt_id            BIGINT PRIMARY KEY DEFAULT nextval('seq_etl_attempts'),
    document_id           VARCHAR NOT NULL,
    krs                   VARCHAR(10),
    started_at            TIMESTAMP NOT NULL DEFAULT current_timestamp,
    finished_at           TIMESTAMP,
    status                VARCHAR NOT NULL,      -- running/completed/failed/skipped
    reason_code           VARCHAR,               -- no_xml_found/parse_error/non_financial_doc/...
    error_message         VARCHAR,
    xml_path              VARCHAR,
    report_id             VARCHAR,
    extraction_version    INTEGER
);

CREATE INDEX IF NOT EXISTS idx_etl_attempts_doc ON etl_attempts(document_id);
CREATE INDEX IF NOT EXISTS idx_etl_attempts_status ON etl_attempts(status);
```

Dodaj widok tylko udanych najnowszych raportów:

```sql
CREATE OR REPLACE VIEW latest_successful_financial_reports AS
SELECT
    id, logical_key, report_version, supersedes_report_id, krs,
    data_source_id, report_type, fiscal_year, period_start, period_end,
    taxonomy_version, source_document_id, source_file_path,
    ingestion_status, ingestion_error, created_at
FROM (
    SELECT
        fr.*,
        row_number() OVER (
            PARTITION BY fr.logical_key
            ORDER BY fr.report_version DESC, fr.created_at DESC, fr.id DESC
        ) AS rn
    FROM financial_reports fr
    WHERE fr.ingestion_status = 'completed'
) ranked
WHERE rn = 1;
```

---

## 6. Uproszczenie bez GUS/GPW (3-6m)

### 6.1 Docelowo usunąć

- `company_identifiers`,
- `seq_company_identifiers`,
- `data_sources` oraz helpery do niego.

### 6.2 Minimalny wariant przejściowy (jeśli trzeba bezpiecznej migracji)

1. Najpierw usuń runtime usage i testy.
2. Potem `DROP TABLE`.
3. `financial_reports.data_source_id` i `bankruptcy_events.data_source_id` zostają jako zwykły `VARCHAR` dla provenance (`'KRS'`).

---

## 7. Plan implementacji (fazy)

## Faza A: Migration-safe foundation (bez łamania API)

1. Dodać tabele:
   - `krs_entity_versions`,
   - `krs_document_versions`,
   - `etl_attempts`.
2. Dodać widoki:
   - `krs_entities_current`,
   - `krs_documents_current`,
   - `latest_successful_financial_reports`.
3. Backfill:
   - `krs_entity_versions` z bieżącego `krs_entities`,
   - `krs_document_versions` z bieżącego `krs_documents` (jako `version_no=1`).
4. Brak usuwania starych tabel na tym etapie.

## Faza B: Zmiany zapisu (write path)

1. `app/repositories/krs_repo.py`:
   - `upsert_entity` zapisuje append-only do `krs_entity_versions`.
   - `get_entity`, `list_stale`, `count_entities` czytają z `krs_entities_current`.
2. `batch/entity_store.py`:
   - zamiast dual-write do `krs_entities` + `krs_registry`, zapis append-only (encja) + update operacyjny `krs_registry`.
3. `app/scraper/db.py`:
   - `insert_documents`, `update_document_metadata`, `mark_downloaded`, `update_document_error` -> append-only do `krs_document_versions` (z hash compare).
4. `batch/rdf_document_store.py`:
   - analogicznie jak `app/scraper/db.py`.

## Faza C: ETL cleanup

1. `app/services/etl.py`:
   - usunąć tworzenie sentinelowych `financial_reports` (`1970-01-01`).
   - logować niepowodzenia do `etl_attempts`.
   - tylko poprawnie sparsowane dokumenty tworzą `financial_reports`.
2. W miejscach czytania "aktualnego raportu" używać `latest_successful_financial_reports` tam, gdzie potrzebna jest jakość produkcyjna.

## Faza D: Uproszczenie multi-source

1. Usunąć z `app/db/prediction_db.py`:
   - schema init `data_sources`, `company_identifiers`, `seq_company_identifiers`,
   - CRUD: `get_data_sources`, `add_company_identifier`.
2. Usunąć/zmienić testy zależne od tych tabel.
3. Migration SQL:
   - `DROP TABLE IF EXISTS company_identifiers;`
   - `DROP SEQUENCE IF EXISTS seq_company_identifiers;`
   - `DROP TABLE IF EXISTS data_sources;`

## Faza E: Cutover read path

1. Wszystkie odczyty encji -> `krs_entities_current`.
2. Wszystkie odczyty dokumentów do pipeline -> `krs_documents_current`.
3. Wykonać porównanie wyników starego i nowego path (sekcja 10).

---

## 8. Lista zmian per plik (obowiązkowa)

- `app/repositories/krs_repo.py`
  - schema init: nowe tabele/widoki,
  - append-only write logic,
  - odczyt z widoku current.
- `app/scraper/db.py`
  - schema init: `krs_document_versions` + widok current,
  - helper `_append_document_version(...)`,
  - refactor update funkcji dokumentu.
- `batch/entity_store.py`
  - append-only encja (bez nadpisywania historii).
- `batch/rdf_document_store.py`
  - append-only wersje dokumentu.
- `app/services/etl.py`
  - `etl_attempts`,
  - brak sentinel `financial_reports`,
  - sensowne statusy (`failed/skipped`).
- `app/db/prediction_db.py`
  - usunięcie `data_sources/company_identifiers`,
  - `latest_successful_financial_reports`.
- `tests/test_prediction_db.py`
  - usunięcie testów multi-source,
  - nowe testy widoku `latest_successful_financial_reports`.
- `tests/test_etl.py`
  - oczekiwanie na `etl_attempts` zamiast sentinelowych raportów failed.
- `docs/database_diagram.mmd`
  - dodać nowe tabele/widoki, usunąć dropped tables.

---

## 9. Backfill i migracja danych (detal)

## 9.1 Backfill `krs_entity_versions`

```sql
INSERT INTO krs_entity_versions (
    krs, name, legal_form, status, registered_at, last_changed_at,
    nip, regon, address_city, address_street, address_postal_code,
    raw, source, valid_from, valid_to, is_current, snapshot_hash, change_reason
)
SELECT
    e.krs, e.name, e.legal_form, e.status, e.registered_at, e.last_changed_at,
    e.nip, e.regon, e.address_city, e.address_street, e.address_postal_code,
    e.raw, e.source,
    coalesce(e.synced_at, current_timestamp),
    NULL,
    true,
    md5(coalesce(cast(e.raw as varchar), '') || '|' || coalesce(e.name, '') || '|' || coalesce(e.legal_form, '')),
    'bootstrap_from_krs_entities'
FROM krs_entities e
LEFT JOIN krs_entity_versions kev ON kev.krs = e.krs AND kev.is_current = true
WHERE kev.krs IS NULL;
```

## 9.2 Backfill `krs_document_versions`

```sql
INSERT INTO krs_document_versions (
    document_id, version_no, krs, rodzaj, status, nazwa, okres_start, okres_end,
    filename, is_ifrs, is_correction, date_filed, date_prepared,
    is_downloaded, downloaded_at, storage_path, storage_backend,
    file_size_bytes, zip_size_bytes, file_count, file_types,
    discovered_at, metadata_fetched_at, download_error,
    valid_from, valid_to, is_current, snapshot_hash, change_reason
)
SELECT
    d.document_id, 1, d.krs, d.rodzaj, d.status, d.nazwa, d.okres_start, d.okres_end,
    d.filename, d.is_ifrs, d.is_correction, d.date_filed, d.date_prepared,
    d.is_downloaded, d.downloaded_at, d.storage_path, d.storage_backend,
    d.file_size_bytes, d.zip_size_bytes, d.file_count, d.file_types,
    d.discovered_at, d.metadata_fetched_at, d.download_error,
    coalesce(d.discovered_at, current_timestamp), NULL, true,
    md5(
        coalesce(d.status,'') || '|' || coalesce(d.filename,'') || '|' ||
        coalesce(cast(d.is_downloaded as varchar),'') || '|' ||
        coalesce(d.download_error,'')
    ),
    'bootstrap_from_krs_documents'
FROM krs_documents d
LEFT JOIN krs_document_versions v ON v.document_id = d.document_id AND v.is_current = true
WHERE v.document_id IS NULL;
```

Uwaga: hash w backfill jest tylko startowy; runtime ma używać jednolitego hashowania pełnego snapshotu.

---

## 10. Testy i kryteria akceptacji

## 10.1 Testy jednostkowe/integracyjne (must)

1. Encja KRS:
   - 2 różne snapshoty => 2 wersje, tylko 1 current.
   - ten sam snapshot 2x => brak nowej wersji.
2. Dokument RDF:
   - insert discovery -> wersja 1,
   - metadata update -> wersja 2,
   - download update -> wersja 3,
   - ponowny identyczny update -> brak nowej wersji.
3. ETL:
   - `no_xml_found` tworzy `etl_attempts(status='failed')`, nie tworzy nowego `financial_reports` sentinela.
4. Widoki:
   - `krs_entities_current` zwraca tylko najnowsze wersje.
   - `krs_documents_current` zwraca tylko najnowsze wersje.
   - `latest_successful_financial_reports` ignoruje failed.

## 10.2 Kontrole SQL po migracji (must)

1. Brak wielokrotnego current per klucz:

```sql
SELECT krs, COUNT(*) FROM krs_entity_versions WHERE is_current = true GROUP BY 1 HAVING COUNT(*) > 1;
SELECT document_id, COUNT(*) FROM krs_document_versions WHERE is_current = true GROUP BY 1 HAVING COUNT(*) > 1;
```

2. Brak nowych raportów sentinelowych:

```sql
SELECT COUNT(*)
FROM financial_reports
WHERE fiscal_year = 0
  AND period_start = '1970-01-01'
  AND period_end = '1970-01-01'
  AND created_at > ?;
```

3. Spójność current view vs cache (jeśli cache zostaje):

```sql
-- porównanie pól krytycznych dla próby danych
```

---

## 11. Rollout i rollback

## 11.1 Rollout

1. Deploy migracji DDL (Faza A).
2. Backfill history tables.
3. Deploy kodu write-path (Faza B).
4. Deploy ETL cleanup (Faza C).
5. Deploy uproszczenia multi-source (Faza D).
6. Cutover read-path (Faza E).
7. Monitoring i walidacja SQL.

## 11.2 Rollback

1. Jeśli write-path powoduje błędy:
   - wrócić do poprzedniej wersji aplikacji,
   - tabele historyczne zostają (non-destructive).
2. Jeśli ETL cleanup powoduje regresję:
   - rollback tylko serwisu ETL,
   - zachować `etl_attempts`.

---

## 12. Definition of Done

Projekt jest zakończony, jeśli:

1. Encje i dokumenty RDF są wersjonowane append-only.
2. Widoki current działają i są używane przez odczyty.
3. Nie ma nowych sentinelowych rekordów `financial_reports`.
4. `company_identifiers` i `data_sources` są usunięte z runtime i migracji.
5. Testy przechodzą, a kontrole SQL z sekcji 10 nie zwracają naruszeń.

---

## 13. Notatka dla implementującego

- W DuckDB nie opieraj spójności na triggerach/FK; kontrolę `is_current` utrzymuj w kodzie (transakcyjnie w jednym połączeniu).
- Hash musi być deterministyczny: ten sam snapshot => ten sam hash.
- Przy cutover zachowaj kompatybilność API; zmieniaj storage warstwowo (schema -> writes -> reads).
