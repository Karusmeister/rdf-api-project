# Frontend API Reference

**Base URL:** `http://localhost:8000` (dev) | To be configured for production.
**Swagger UI:** `{base}/docs` | **OpenAPI JSON:** `{base}/openapi.json`
**Auth mechanism:** JWT Bearer tokens in `Authorization: Bearer <token>` header.

This document describes every API call the frontend should use, organized by the user journey. Each endpoint is annotated with **who** calls it (public page, logged-in user, admin) and **when** in the UI flow.

---

## Table of Contents

1. [User Roles and Access Model](#1-user-roles-and-access-model)
2. [Authentication Flow](#2-authentication-flow)
3. [Predictions (Core Feature)](#3-predictions-core-feature)
4. [Company Lookup and Documents](#4-company-lookup-and-documents)
5. [Financial Statement Analysis](#5-financial-statement-analysis)
6. [System Status (Admin/Debug)](#6-system-status-admindebug)
7. [Error Handling](#7-error-handling)
8. [Rate Limits](#8-rate-limits)
9. [Response Shape Reference](#9-response-shape-reference)

---

## 1. User Roles and Access Model

| Role | Description | Access |
|------|-------------|--------|
| **Anonymous** | Not logged in | Can view model catalog, look up KRS entities, browse documents |
| **User** | Logged in, verified | Everything anonymous can do + predictions for KRS numbers they have access to |
| **Admin** | `has_full_access: true` | Everything + predictions for ALL KRS numbers + grant access to other users + cache management |

**KRS access model:** Users can only query predictions for KRS numbers explicitly granted to them via `/api/auth/admin/grant-access`. Admins bypass this — they see everything. The `/api/auth/me` endpoint returns the user's `krs_access` list so the frontend knows which companies to show.

---

## 2. Authentication Flow

### 2a. Email/Password Signup

**Who:** Anonymous user on the registration page.

**Flow:** Signup -> check email for 6-digit code -> verify -> logged in.

#### Step 1: Register

```
POST /api/auth/signup
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `email` | string (email) | yes | User's email |
| `password` | string | yes | Min 8 characters |
| `name` | string | no | Display name |

**Response (200):**
```json
{
  "message": "Verification code sent",
  "user_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Frontend notes:**
- Store `user_id` in component state — needed for the verify step.
- If the user already signed up but hasn't verified, calling signup again resends a new code (no 409).
- **409** = email already registered AND verified. Show "Already have an account? Log in."
- **503** = email delivery failed. Show retry button.

#### Step 2: Verify email

```
POST /api/auth/verify
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `user_id` | string | yes | From signup response |
| `code` | string | yes | Exactly 6 digits |

**Response (200):**
```json
{
  "token": "eyJhbGciOi...",
  "user": {
    "id": "550e8400-...",
    "email": "user@example.com",
    "name": "Jan Kowalski",
    "has_full_access": false,
    "krs_access": []
  }
}
```

**Frontend notes:**
- Store `token` in localStorage or secure cookie.
- Set `Authorization: Bearer <token>` on all subsequent requests.
- **400** = wrong or expired code. Show "Invalid code" with retry.
- Code expires after 15 minutes.

### 2b. Email/Password Login

**Who:** Returning user on the login page.

```
POST /api/auth/login
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `email` | string (email) | yes | Registered email |
| `password` | string | yes | Account password |

**Response (200):** Same `AuthResponse` shape as verify.

**Error codes:**
- **401** = wrong email or password. Show generic "Invalid credentials."
- **400** = "This account uses Google sign-in" — redirect to Google SSO.
- **403** = "Email not verified" — redirect to verification page (let them re-signup to resend code).

### 2c. Google SSO

**Who:** User clicking "Sign in with Google" button.

```
POST /api/auth/google
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id_token` | string | yes | Google OAuth2 ID token from client-side `google.accounts.id.initialize()` |

**Response (200):** Same `AuthResponse` shape. Auto-creates and verifies user on first login.

**Frontend notes:**
- Use Google Identity Services (GIS) library to get the `id_token` client-side.
- No signup/verify step needed — one call and the user is in.
- The `GOOGLE_CLIENT_ID` used server-side must match the one in your GIS config.

### 2d. Get Current User Profile

**Who:** Logged-in user. Call on app mount to validate token and load permissions.

```
GET /api/auth/me
Authorization: Bearer <token>
```

**Response (200):**
```json
{
  "id": "550e8400-...",
  "email": "user@example.com",
  "name": "Jan Kowalski",
  "has_full_access": false,
  "krs_access": ["0000694720", "0000012345"]
}
```

**Frontend notes:**
- Call this on app startup to check if the stored token is still valid.
- **401** = token expired or invalid. Clear stored token, redirect to login.
- `has_full_access: true` means admin — show admin UI elements.
- `krs_access` is the list of KRS numbers this user can query predictions for. Use it to populate the company selector/dashboard.

### 2e. Grant KRS Access (Admin)

**Who:** Admin user on a user management page.

```
POST /api/auth/admin/grant-access
Authorization: Bearer <token>
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `user_id` | string | yes | Target user's ID |
| `krs` | string | yes | KRS number (1-10 digits) |

**Response (200):** `{"granted": true}`

**Error codes:**
- **403** = caller is not an admin.
- **404** = target user_id doesn't exist.

---

## 3. Predictions (Core Feature)

### 3a. Model Catalog

**Who:** Anyone (public). Show on a "Models" or "About" page explaining what models are available.

```
GET /api/predictions/models
```

**No auth required.**

**Response (200):**
```json
{
  "models": [
    {
      "model_id": "maczynska_1994_v1",
      "model_name": "maczynska",
      "model_type": "discriminant",
      "model_version": "1994_v1",
      "is_baseline": true,
      "description": "Maczynska 1994 Z-score model",
      "feature_set_id": "maczynska_6",
      "interpretation": {
        "score_name": "Z-score (Zm)",
        "higher_is_better": true,
        "thresholds": [
          {"label": "critical", "max": 0, "summary": "Bankruptcy zone.", "is_current": false},
          {"label": "high", "min": 0, "max": 1, "summary": "Weak condition.", "is_current": false},
          {"label": "medium", "min": 1, "max": 2, "summary": "Acceptable.", "is_current": false},
          {"label": "low", "min": 2, "summary": "Good condition.", "is_current": false}
        ]
      }
    }
  ]
}
```

**Frontend notes:**
- Use `interpretation.thresholds` to render a score gauge/legend.
- `is_baseline: true` marks the primary model — show it first.
- `higher_is_better` tells you which direction is "good" for color coding.

### 3b. Full Prediction for a Company

**Who:** Logged-in user with access to this KRS. The main company detail/dashboard page.

```
GET /api/predictions/{krs}
Authorization: Bearer <token>
```

**Path param:** `krs` — 1-10 digits (e.g. `0000694720` or `694720`).

**Response (200):**
```json
{
  "company": {
    "krs": "0000694720",
    "name": null,
    "nip": "1234567890",
    "pkd_code": "62.01.Z"
  },
  "predictions": [
    {
      "model": {
        "model_id": "maczynska_1994_v1",
        "model_name": "maczynska",
        "model_type": "discriminant",
        "model_version": "1994_v1",
        "is_baseline": true,
        "description": "Maczynska 1994 Z-score model"
      },
      "result": {
        "raw_score": 2.534,
        "probability": null,
        "classification": 0,
        "risk_category": "low"
      },
      "interpretation": {
        "score_name": "Z-score (Zm)",
        "higher_is_better": true,
        "thresholds": [
          {"label": "critical", "max": 0, "summary": "Bankruptcy zone.", "is_current": false},
          {"label": "high", "min": 0, "max": 1, "summary": "Weak condition.", "is_current": false},
          {"label": "medium", "min": 1, "max": 2, "summary": "Acceptable.", "is_current": false},
          {"label": "low", "min": 2, "summary": "Good condition.", "is_current": true}
        ]
      },
      "features": [
        {
          "feature_id": "x1_maczynska",
          "name": "X1 (Maczynska)",
          "category": "profitability",
          "value": 0.35,
          "contribution": 0.577,
          "formula_description": "(Gross profit + Depreciation) / Total liabilities",
          "source_tags": [
            {"tag_path": "RZiS.I", "label_pl": "Zysk (strata) brutto", "value_current": 200000, "value_previous": 180000, "section": "RZiS"},
            {"tag_path": "Pasywa_B", "label_pl": "Zobowiazania i rezerwy", "value_current": 500000, "value_previous": 450000, "section": "Bilans"}
          ]
        }
      ],
      "data_source": {
        "report_id": "rpt-123",
        "fiscal_year": 2024,
        "period_start": "2024-01-01",
        "period_end": "2024-12-31",
        "report_version": 1,
        "data_source_id": "KRS",
        "ingested_at": "2026-03-15 10:00:00"
      },
      "scored_at": "2026-03-20 12:00:00"
    }
  ],
  "history": [
    {
      "model_id": "maczynska_1994_v1",
      "model_name": "maczynska",
      "model_version": "1994_v1",
      "fiscal_year": 2023,
      "raw_score": 2.1,
      "probability": null,
      "classification": 0,
      "risk_category": "low",
      "scored_at": "2026-02-01 09:00:00"
    },
    {
      "model_id": "maczynska_1994_v1",
      "model_name": "maczynska",
      "model_version": "1994_v1",
      "fiscal_year": 2024,
      "raw_score": 2.534,
      "probability": null,
      "classification": 0,
      "risk_category": "low",
      "scored_at": "2026-03-20 12:00:00"
    }
  ]
}
```

**Frontend notes:**
- `predictions[]` = latest score per active model. Most companies have one (Maczynska baseline).
- `predictions[].features[]` is the full feature breakdown — render as a table with expandable source tags.
- `predictions[].interpretation.thresholds[].is_current` tells you which band to highlight.
- `predictions[].result.risk_category` drives the top-level badge color: `critical`=red, `high`=orange, `medium`=yellow, `low`=green.
- `history[]` = all historical scores ordered by `fiscal_year`. Use for a line chart.
- `company.name` may be `null` if the company was discovered by KRS number only. `nip` and `pkd_code` are more reliable.
- **403** = user doesn't have access to this KRS. Show "Request access from admin."
- **404** = no data at all for this KRS (not in our system).

### 3c. Score History (for charts)

**Who:** Logged-in user viewing a score trend chart. Use when you only need the timeline, not full feature detail.

```
GET /api/predictions/{krs}/history?model_id=maczynska_1994_v1
Authorization: Bearer <token>
```

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `krs` | path | yes | KRS number |
| `model_id` | query | no | Filter to one model. Omit for all models. |

**Response (200):**
```json
{
  "krs": "0000694720",
  "history": [
    {"model_id": "maczynska_1994_v1", "fiscal_year": 2022, "raw_score": 1.8, "risk_category": "medium", ...},
    {"model_id": "maczynska_1994_v1", "fiscal_year": 2023, "raw_score": 2.1, "risk_category": "low", ...},
    {"model_id": "maczynska_1994_v1", "fiscal_year": 2024, "raw_score": 2.534, "risk_category": "low", ...}
  ]
}
```

**Frontend notes:**
- Ordered by `fiscal_year` ASC — ready for charting.
- If multiple models exist, group by `model_id` for multi-line charts.

### 3d. Invalidate Caches (Admin)

**Who:** Admin after seeding new models or recomputing features.

```
POST /api/predictions/cache/invalidate
Authorization: Bearer <token>
```

**Response (200):** `{"status": "caches_invalidated"}`

---

## 4. Company Lookup and Documents

These endpoints proxy the Polish RDF registry. **No auth required** — useful for a public search page.

### 4a. Look Up a Company

**Who:** Anyone on a search/lookup page.

```
POST /api/podmiot/lookup
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `krs` | string | yes | 1-10 digits |

**Response (200):**
```json
{
  "podmiot": {
    "numer_krs": "0000694720",
    "nazwa_podmiotu": "EXAMPLE SP. Z O.O.",
    "forma_prawna": "SPOLKA Z OGRANICZONA ODPOWIEDZIALNOSCIA",
    "wykreslenie": "NIE"
  },
  "czy_podmiot_znaleziony": true,
  "komunikat_bledu": null
}
```

**Frontend notes:**
- `czy_podmiot_znaleziony: false` = KRS not found. Show "Company not found."
- `wykreslenie: "TAK"` = company has been deregistered. Show a warning badge.

### 4b. Search Documents

**Who:** Anyone browsing a company's filings.

```
POST /api/dokumenty/search
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `krs` | string | yes | | 1-10 digits |
| `page` | int | no | 0 | Page number (0-based) |
| `page_size` | int | no | 10 | 1-100 |
| `sort_field` | string | no | "id" | Sort field |
| `sort_dir` | string | no | "MALEJACO" | "MALEJACO" (newest first) or "ROSNACO" |

**Response (200):**
```json
{
  "content": [
    {
      "id": "ZgsX8Fsncb1PFW07-T4XoQ==",
      "rodzaj": "18",
      "status": "NIEUSUNIETY",
      "nazwa": "Sprawozdanie finansowe",
      "okres_sprawozdawczy_poczatek": "2023-01-01",
      "okres_sprawozdawczy_koniec": "2023-12-31",
      "status_bezpieczenstwa": null,
      "data_usuniecia_dokumentu": null
    }
  ],
  "metadane_wynikow": {
    "numer_strony": 0,
    "rozmiar_strony": 10,
    "liczba_stron": 3,
    "calkowita_liczba_obiektow": 25
  }
}
```

**Frontend notes:**
- `rodzaj: "18"` = financial statement (the type we analyze). Other codes are different document types.
- `status: "NIEUSUNIETY"` = active. "USUNIETY" = deleted.
- Use `metadane_wynikow` for pagination controls.

### 4c. Get Document Metadata

**Who:** Anyone clicking on a specific document.

```
GET /api/dokumenty/metadata/{doc_id}
```

**Note:** `doc_id` is Base64 and must be URL-encoded. The ID from search results can contain `=`, `+`, `/`.

**Response (200):** Raw JSON from upstream RDF registry (shape varies).

### 4d. Download Documents as ZIP

**Who:** User clicking "Download" on one or more documents.

```
POST /api/dokumenty/download
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `document_ids` | string[] | yes | 1-20 document IDs from search results |

**Response:** Binary ZIP file (`Content-Type: application/zip`).

**Frontend notes:**
- Use `fetch` + `blob()` + `URL.createObjectURL()` to trigger browser download.
- Or use a hidden form submission.

---

## 5. Financial Statement Analysis

These endpoints download and parse Polish GAAP XML statements server-side. **No auth required.** Useful for a statement viewer/comparison page.

### 5a. List Available Periods

**Who:** Anyone viewing a company's statement history.

```
GET /api/analysis/available-periods/{krs}
```

**Response (200):**
```json
{
  "krs": "0000694720",
  "company_name": "EXAMPLE SP. Z O.O.",
  "periods": [
    {"period_start": "2022-01-01", "period_end": "2022-12-31", "document_id": "abc==", "is_correction": false, "is_ifrs": false},
    {"period_start": "2023-01-01", "period_end": "2023-12-31", "document_id": "def==", "is_correction": true, "is_ifrs": false}
  ]
}
```

**Frontend notes:**
- `is_correction: true` = this filing corrects a previous submission for the same period.
- `is_ifrs: true` would be IFRS — currently filtered out (only Polish GAAP supported).
- Use `period_end` values when calling statement/compare/time-series endpoints.

### 5b. Parse a Statement

**Who:** Anyone viewing a single statement detail page.

```
POST /api/analysis/statement
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `krs` | string | yes | 1-10 digits |
| `period_end` | string | no | "YYYY-MM-DD". Omit for most recent. |

**Response (200):** Hierarchical tree with `bilans` (balance sheet), `rzis` (income statement), `cash_flow` sections. Each node has `kwota_a` (current) and `kwota_b` (previous period).

### 5c. Compare Two Periods

**Who:** Anyone on a YoY comparison page.

```
POST /api/analysis/compare
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `krs` | string | yes | 1-10 digits |
| `period_end_current` | string | yes | Current period (YYYY-MM-DD) |
| `period_end_previous` | string | yes | Previous period (YYYY-MM-DD) |

**Response (200):** Merged tree with change calculations + financial ratios (equity, current, debt, margins).

### 5d. Time Series

**Who:** Anyone viewing a multi-year chart for specific financial items.

```
POST /api/analysis/time-series
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `krs` | string | yes | 1-10 digits |
| `fields` | string[] | yes | Tag paths: `["Aktywa", "Pasywa_A", "RZiS.A", "RZiS.L"]` |
| `period_ends` | string[] | no | Filter to specific years. Omit for all. |

**Response (200):**
```json
{
  "company": {"name": "EXAMPLE SP. Z O.O.", "krs": "0000694720"},
  "periods": [
    {"start": "2021-01-01", "end": "2021-12-31"},
    {"start": "2022-01-01", "end": "2022-12-31"},
    {"start": "2023-01-01", "end": "2023-12-31"}
  ],
  "series": [
    {
      "tag": "Aktywa",
      "label": "Aktywa razem",
      "section": "bilans",
      "values": [900000, 1000000, 1100000],
      "changes_absolute": [null, 100000, 100000],
      "changes_percent": [null, 11.11, 10.0]
    }
  ]
}
```

**Frontend notes:**
- `periods[]` and `series[].values[]` are aligned by index — period[0] corresponds to values[0].
- Common tag paths: `Aktywa` (total assets), `Pasywa_A` (equity), `Pasywa_B` (liabilities), `RZiS.A` (revenue), `RZiS.L` (net profit/loss), `CF.D` (net cash change).

---

## 6. System Status (Admin/Debug)

### 6a. Health Check

```
GET /health
```
**Response:** `{"status": "ok"}`

### 6b. KRS Adapter Health

```
GET /health/krs
```
**Response (200 or 503):** Adapter connectivity status.

### 6c. Scraper Status

```
GET /api/scraper/status
```
**Response:** Aggregate stats (total KRS, documents downloaded, etc.) and last run info.

### 6d. KRS Sync/Scanner Jobs

| Method | Path | Description |
|--------|------|-------------|
| GET | `/jobs/krs-sync/status` | Last sync run summary |
| POST | `/jobs/krs-sync/trigger` | Queue a sync run (202/409) |
| GET | `/jobs/krs-scan/status` | Scanner cursor, running status, last run |
| POST | `/jobs/krs-scan/trigger` | Start scan (202/409) |
| POST | `/jobs/krs-scan/stop` | Signal scan to stop |
| POST | `/jobs/krs-scan/reset-cursor` | Reset cursor position |

**Frontend notes:** These are operational/admin endpoints. You probably don't need them in the main user-facing UI — they're useful for an admin dashboard.

---

## 7. Error Handling

All errors return JSON with a `detail` field:

```json
{"detail": "Error message here"}
```

| Status | Meaning | Frontend action |
|--------|---------|-----------------|
| **400** | Bad request (validation, wrong code) | Show error message from `detail` |
| **401** | Not authenticated or token expired | Clear token, redirect to login |
| **403** | Insufficient permissions | Show "Access denied" or "Request access" |
| **404** | Resource not found | Show "Not found" state |
| **409** | Conflict (duplicate email, job already running) | Show conflict message |
| **422** | Validation error (Pydantic) | `detail` is an array of field-level errors |
| **429** | Rate limited | Show "Too many requests. Please wait." |
| **502** | Upstream RDF API error | Show "Data source temporarily unavailable" |
| **503** | Service unavailable (SMTP failure, etc.) | Show retry option |

**422 shape** (Pydantic validation):
```json
{
  "detail": [
    {"loc": ["body", "email"], "msg": "value is not a valid email address", "type": "value_error"}
  ]
}
```

---

## 8. Rate Limits

| Endpoint | Limit | Scope |
|----------|-------|-------|
| `POST /api/auth/signup` | 5/minute | Per IP |
| `POST /api/auth/verify` | 10/minute | Per IP |
| `POST /api/auth/admin/grant-access` | 20/minute | Per IP |

When exceeded, the API returns **429 Too Many Requests**. The `Retry-After` header indicates when to retry.

---

## 9. Response Shape Reference

### AuthResponse
```typescript
interface AuthResponse {
  token: string;             // JWT — store and send as Bearer token
  user: UserProfile;
}
```

### UserProfile
```typescript
interface UserProfile {
  id: string;                // UUID
  email: string;
  name: string | null;
  has_full_access: boolean;  // true = admin
  krs_access: string[];      // KRS numbers this user can query
}
```

### PredictionResponse
```typescript
interface PredictionResponse {
  company: {
    krs: string;             // zero-padded 10 digits
    name: string | null;
    nip: string | null;
    pkd_code: string | null;
  };
  predictions: PredictionDetail[];  // latest score per model
  history: HistoryEntry[];          // all scores for timeline
}
```

### PredictionDetail
```typescript
interface PredictionDetail {
  model: {
    model_id: string;
    model_name: string;
    model_type: string;      // "discriminant", "logistic", etc.
    model_version: string;
    is_baseline: boolean;
    description: string | null;
  };
  result: {
    raw_score: number | null;
    probability: number | null;
    classification: number | null;  // 0=healthy, 1=risk
    risk_category: string | null;   // "critical" | "high" | "medium" | "low"
  };
  interpretation: {
    score_name: string;
    higher_is_better: boolean;
    thresholds: {
      label: string;
      min: number | null;
      max: number | null;
      summary: string;
      is_current: boolean;   // highlight this band in the UI
    }[];
  } | null;
  features: {
    feature_id: string;
    name: string;
    category: string | null;
    value: number | null;
    contribution: number | null;
    formula_description: string | null;
    source_tags: {
      tag_path: string;
      label_pl: string | null;
      value_current: number | null;
      value_previous: number | null;
      section: string | null;
    }[];
  }[];
  data_source: {
    report_id: string;
    fiscal_year: number;
    period_start: string | null;
    period_end: string | null;
    report_version: number;
    ingested_at: string | null;
  };
  scored_at: string | null;
}
```

### HistoryEntry
```typescript
interface HistoryEntry {
  model_id: string;
  model_name: string;
  model_version: string;
  fiscal_year: number;
  raw_score: number | null;
  probability: number | null;
  classification: number | null;
  risk_category: string | null;
  scored_at: string | null;
}
```

### ModelsResponse
```typescript
interface ModelsResponse {
  models: {
    model_id: string;
    model_name: string;
    model_type: string;
    model_version: string;
    is_baseline: boolean;
    description: string | null;
    feature_set_id: string | null;
    interpretation: InterpretationDetail | null;
  }[];
}
```
