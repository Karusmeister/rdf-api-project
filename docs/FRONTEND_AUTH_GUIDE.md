# Frontend Authentication Guide

Backend API reference for the Lovable (React) frontend. All endpoints are under `/api/auth`.

**Base URL (production):** `https://rdf-api-<hash>-europe-central2.a.run.app`
**Base URL (local):** `http://localhost:8000`

---

## Authentication Flow Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                                                                      │
│  1. REGISTRATION                                                     │
│     POST /api/auth/register                                          │
│     { email, password, name?, captcha_token? }                       │
│              │                                                       │
│              ▼                                                       │
│     ← 200 { message, user_id }                                      │
│     (verification code sent to email)                                │
│                                                                      │
│  2. EMAIL VERIFICATION                                               │
│     POST /api/auth/verify                                            │
│     { user_id, code }  (6-digit code from email)                     │
│              │                                                       │
│              ▼                                                       │
│     ← 200 { token, user }   ◄── store token in localStorage         │
│                                                                      │
│  3. LOGIN (returning users)                                          │
│     POST /api/auth/login                                             │
│     { email, password, captcha_token? }                              │
│              │                                                       │
│              ▼                                                       │
│     ← 200 { token, user }   ◄── store token in localStorage         │
│                                                                      │
│  4. AUTHENTICATED REQUESTS                                           │
│     GET /api/auth/me                                                 │
│     Headers: { Authorization: "Bearer <token>" }                     │
│              │                                                       │
│              ▼                                                       │
│     ← 200 { id, email, name, has_full_access, krs_access[] }        │
│                                                                      │
│  5. PASSWORD RESET (if forgotten)                                    │
│     POST /api/auth/forgot-password  → email with reset link          │
│     POST /api/auth/reset-password   → set new password               │
│                                                                      │
│  6. GOOGLE SSO (alternative)                                         │
│     POST /api/auth/google                                            │
│     { id_token }  → auto-creates verified user, returns JWT          │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Endpoint Reference

### POST `/api/auth/register`

Create a new account. Sends a 6-digit verification code to the email.

**Request:**
```json
{
  "email": "user@example.com",
  "password": "securepass123",
  "name": "Jan Kowalski",
  "captcha_token": "reCAPTCHA-v3-token-from-frontend"
}
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `email` | string | yes | Must be valid email format |
| `password` | string | yes | Minimum 8 characters |
| `name` | string | no | Display name |
| `captcha_token` | string | no* | reCAPTCHA v3 token. *Required in production when `RECAPTCHA_SECRET_KEY` is set |

**Success response (200):**
```json
{
  "message": "Verification code sent",
  "user_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**The user cannot log in yet.** The frontend must show a verification code input screen and call `/verify` with the `user_id` and the 6-digit code from their email.

**Error responses:**

| Status | Meaning | Frontend action |
|--------|---------|-----------------|
| 409 | Email already registered | Show "email taken" message, link to login |
| 422 | Validation error (short password, bad email) | Show field-level errors |
| 400 | reCAPTCHA failed | Retry with fresh token |
| 429 | Rate limited (5/min) | Show "try again later" |
| 503 | Email delivery failed | Show "try again" |

**If the user registered but never verified:** Calling register again with the same email will resend a new verification code (not create a duplicate account).

---

### POST `/api/auth/verify`

Submit the 6-digit code from the verification email. Returns a JWT on success.

**Request:**
```json
{
  "user_id": "550e8400-e29b-41d4-a716-446655440000",
  "code": "123456"
}
```

**Success response (200):**
```json
{
  "token": "eyJhbGciOiJIUzI1NiIs...",
  "user": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "email": "user@example.com",
    "name": "Jan Kowalski",
    "has_full_access": false,
    "krs_access": []
  }
}
```

**Store `token` in localStorage.** The user is now logged in.

| Status | Meaning | Frontend action |
|--------|---------|-----------------|
| 400 | Wrong or expired code | Show error, allow retry |
| 429 | Rate limited (10/min) | Show "try again later" |

**Code expiry:** 15 minutes. After that, the user must re-register (which resends a new code).

---

### POST `/api/auth/login`

Authenticate a verified user.

**Request:**
```json
{
  "email": "user@example.com",
  "password": "securepass123",
  "captcha_token": "reCAPTCHA-v3-token"
}
```

**Success response (200):**
```json
{
  "token": "eyJhbGciOiJIUzI1NiIs...",
  "user": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "email": "user@example.com",
    "name": "Jan Kowalski",
    "has_full_access": false,
    "krs_access": ["0000694720"]
  }
}
```

| Status | Meaning | Frontend action |
|--------|---------|-----------------|
| 401 | Wrong email or password | Show generic "invalid credentials" |
| 400 | Google SSO account (no password) | Show "use Google sign-in" |
| 403 | Email not verified | Show "check inbox for code", link to verify screen |
| 403 | Account deactivated | Show "account deactivated" |
| 400 | reCAPTCHA failed | Retry with fresh token |

---

### POST `/api/auth/google`

Exchange a Google OAuth2 ID token for a JWT. Auto-creates and verifies the user on first login (no verification step needed).

**Request:**
```json
{
  "id_token": "eyJhbGciOiJSUzI1NiIs..."
}
```

**Success response (200):** Same `{ token, user }` structure as login.

---

### GET `/api/auth/me`

Get the current user's profile. Use this on app load to check if the stored JWT is still valid.

**Headers:**
```
Authorization: Bearer eyJhbGciOiJIUzI1NiIs...
```

**Success response (200):**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "email": "user@example.com",
  "name": "Jan Kowalski",
  "has_full_access": false,
  "krs_access": ["0000694720", "0000123456"]
}
```

| Status | Meaning | Frontend action |
|--------|---------|-----------------|
| 401 | Token missing, expired, or invalid | Clear localStorage, redirect to login |

**Use `has_full_access`** to show/hide admin features. **Use `krs_access`** to show which KRS numbers the user can query.

---

### POST `/api/auth/forgot-password`

Request a password reset email. **Always returns 200** regardless of whether the email exists (security: don't leak user existence).

**Request:**
```json
{
  "email": "user@example.com",
  "captcha_token": "reCAPTCHA-v3-token"
}
```

**Response (always 200):**
```json
{
  "message": "If this email is registered, a reset link has been sent."
}
```

The email contains a link: `{FRONTEND_URL}/reset-password?token={token}`

The frontend must have a `/reset-password` page that reads the `token` query parameter.

---

### POST `/api/auth/reset-password`

Set a new password using the token from the reset email.

**Request:**
```json
{
  "token": "abc123def456...",
  "new_password": "newSecurePass!"
}
```

**Success response (200):**
```json
{
  "message": "Password updated successfully"
}
```

| Status | Meaning | Frontend action |
|--------|---------|-----------------|
| 400 | Token invalid, expired, or already used | Show "link expired, request a new one" |
| 422 | Password too short (<8 chars) | Show validation error |

**Token expiry:** 1 hour. Single-use — once consumed, it cannot be reused.

---

## JWT Token Handling

### Storage
Store the JWT in `localStorage`:
```javascript
localStorage.setItem("token", response.token);
```

### Sending with requests
Include in every authenticated API call:
```javascript
fetch("/api/predictions/0000694720", {
  headers: {
    "Authorization": `Bearer ${localStorage.getItem("token")}`,
    "Content-Type": "application/json",
  },
});
```

### Token lifetime
Tokens expire after **24 hours**. After expiry, `/api/auth/me` returns 401 — redirect to login.

### Logout
Clear the token from localStorage. There is no server-side logout endpoint (JWT is stateless).
```javascript
localStorage.removeItem("token");
window.location.href = "/login";
```

---

## reCAPTCHA v3 Integration

The backend verifies reCAPTCHA tokens on `/register`, `/login`, and `/forgot-password`.

### Frontend setup
1. Load the reCAPTCHA v3 script with your **site key**
2. On form submit, call `grecaptcha.execute(siteKey, { action: "register" })` (or `"login"`, `"forgot_password"`)
3. Pass the token as `captcha_token` in the request body

### Dev mode
When `RECAPTCHA_SECRET_KEY` is not set on the backend (local dev), captcha verification is skipped. You can omit `captcha_token` or send any value.

### Actions
| Endpoint | reCAPTCHA action string |
|----------|------------------------|
| `/register` | `"register"` |
| `/login` | `"login"` |
| `/forgot-password` | `"forgot_password"` |

---

## Recommended Frontend Page Flow

```
/login
  ├── Form: email, password, captcha
  ├── Submit → POST /api/auth/login
  ├── On 200 → store token, redirect to /search
  ├── On 403 "not verified" → redirect to /verify?user_id=...
  └── Link: "Forgot password?" → /forgot-password
       └── Link: "Don't have an account?" → /register

/register
  ├── Form: email, password, name (optional), captcha
  ├── Submit → POST /api/auth/register
  ├── On 200 → redirect to /verify?user_id={user_id}
  └── On 409 → show "email taken", link to /login

/verify
  ├── Form: 6-digit code input
  ├── user_id from URL params (passed from /register)
  ├── Submit → POST /api/auth/verify
  └── On 200 → store token, redirect to /search

/forgot-password
  ├── Form: email, captcha
  ├── Submit → POST /api/auth/forgot-password
  └── On 200 → show "check your email"

/reset-password?token=...
  ├── Read token from URL query string
  ├── Form: new password, confirm password
  ├── Submit → POST /api/auth/reset-password
  └── On 200 → show "password updated", link to /login

App initialization (every page load):
  ├── Check localStorage for token
  ├── If token exists → GET /api/auth/me
  │   ├── On 200 → user is authenticated, proceed
  │   └── On 401 → clear token, redirect to /login
  └── If no token → redirect to /login (for protected routes)
```

---

## Access Control

| `has_full_access` | `krs_access` | What the user can do |
|-------------------|--------------|----------------------|
| `true` | (ignored) | Admin. Can access all KRS numbers and admin endpoints |
| `false` | `["0000694720"]` | Can only query predictions for granted KRS numbers |
| `false` | `[]` | Authenticated but no KRS access yet (show "request access" UI) |

Admin grants access via `POST /api/auth/admin/grant-access`:
```json
{
  "user_id": "target-user-id",
  "krs": "0000694720"
}
```

---

## Error Response Format

All error responses follow this structure:
```json
{
  "detail": "Human-readable error message"
}
```

For validation errors (422):
```json
{
  "detail": [
    {
      "loc": ["body", "password"],
      "msg": "String should have at least 8 characters",
      "type": "string_too_short"
    }
  ]
}
```

---

## Environment Variables (Backend)

| Variable | Purpose | Required |
|----------|---------|----------|
| `RECAPTCHA_SECRET_KEY` | Google reCAPTCHA v3 secret key | **Required** in staging/production |
| `AUTH_REQUIRE_CAPTCHA_IN_NONLOCAL` | Enforce captcha requirement (default: `true`) | No |
| `FRONTEND_URL` | Base URL for password reset links. **Must be `https://`** in staging/production | **Required** in staging/production |
| `JWT_SECRET` | JWT signing secret (>=32 bytes in production) | Always |
| `VERIFICATION_EMAIL_MODE` | `log` (dev, prints to console) or `smtp` (sends real email). **Must be `smtp`** in staging/production | Always |
| `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM` | Email delivery | When `VERIFICATION_EMAIL_MODE=smtp` |
| `GOOGLE_CLIENT_ID` | Google OAuth2 client ID | For Google SSO |

### Non-local enforcement

The backend **refuses to start** in staging/production if:
- `RECAPTCHA_SECRET_KEY` is empty (unless `AUTH_REQUIRE_CAPTCHA_IN_NONLOCAL=false`)
- `VERIFICATION_EMAIL_MODE` is `log`
- `FRONTEND_URL` does not use `https://` or points to localhost

These checks do not apply in `ENVIRONMENT=local`.
