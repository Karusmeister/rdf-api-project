# Agent Instructions: Build RDF FastAPI Service

> Ten dokument zawiera instrukcje dla agenta AI (Claude, Cursor, Copilot, itp.)
> do zbudowania produkcyjnego serwisu FastAPI stanowiacego proxy do API
> Repozytorium Dokumentow Finansowych (rdf-przegladarka.ms.gov.pl).

---

## Cel

Zbuduj asynchroniczny serwis FastAPI, ktory:
1. Przyjmuje prosty numer KRS od klienta
2. Obsluguje szyfrowanie AES-128-CBC wymagane przez API RDF
3. Proxy-uje requesty do upstream API MS.GOV.PL
4. Zwraca czyste JSON-y i pliki ZIP klientowi
5. Dziala wielowatkowo (multiple workers) pod obciazeniem

---

## Kontekst techniczny

### Upstream API

Base URL: `https://rdf-przegladarka.ms.gov.pl/services/rdf/przegladarka-dokumentow-finansowych`

Flow:
```
POST /podmioty/wyszukiwanie/dane-podstawowe    { "numerKRS": "0000694720" }
POST /dokumenty/rodzajeDokWyszukiwanie         { "nrKRS": "0000694720" }
POST /dokumenty/wyszukiwanie                   { "nrKRS": "<ENCRYPTED>", "metadaneStronicowania": {...} }
GET  /dokumenty/{id}                           (URL-encode Base64 id)
POST /dokumenty/tresc                          ["<doc_id>"]  -> ZIP binary
```

### Szyfrowanie nrKRS (krytyczne)

Endpoint `/dokumenty/wyszukiwanie` wymaga zaszyfrowanego numeru KRS.

Algorytm (z frontendu `main-C7XHMT4M.js`, funkcja `encryptNrKrs`):

```
INPUT:  krs = "0000694720"
NOW:    datetime.now()

PLAINTEXT:
  timestamp_full = now.strftime("%Y-%m-%d-%H-%M-%S")     # "2026-03-19-14-30-45"
  plaintext      = krs.zfill(10) + timestamp_full         # "00006947202026-03-19-14-30-45"

KEY & IV (identyczne):
  timestamp_hour = now.strftime("%Y-%m-%d-%H")            # "2026-03-19-14"
  key = timestamp_hour.rjust(16, "1")                     # "1112026-03-19-14"
  iv  = key                                               # ten sam string

ENCRYPTION:
  AES-128-CBC, padding PKCS7
  output = base64_encode(ciphertext)

RESULT: "IxW7jON1dHOJSvhGjTLouRR0zd0tTAfUHWXl1rApR5Q="
```

WAZNE: klucz zmienia sie co godzine, plaintext co sekunde. Token musi byc generowany
tuz przed wyslaniem requestu - nie cache'uj go.

---

## Krok po kroku

### Krok 1: Inicjalizacja projektu

Utworz strukture katalogow:

```
rdf-api/
  app/
    __init__.py
    main.py
    config.py
    crypto.py
    rdf_client.py
    schemas.py
    exceptions.py
    routers/
      __init__.py
      podmiot.py
      dokumenty.py
  requirements.txt
  Dockerfile
  .env.example
  tests/
    __init__.py
    test_crypto.py
    test_endpoints.py
```

Zaleznosci (`requirements.txt`):
```
fastapi>=0.115
uvicorn[standard]>=0.34
httpx>=0.28
pycryptodome>=3.21
pydantic>=2.10
pydantic-settings>=2.7
pytest>=8.0
pytest-asyncio>=0.24
httpx  # also used by pytest as test client
```

### Krok 2: Config (`app/config.py`)

Uzyj `pydantic-settings` do konfiguracji z env vars:

```
RDF_BASE_URL        - URL upstream API (domyslnie: pelny URL powyzej)
RDF_REFERER         - wartosc naglowka Referer
RDF_ORIGIN          - wartosc naglowka Origin
REQUEST_TIMEOUT     - timeout w sekundach (domyslnie: 30)
MAX_CONNECTIONS     - limit polaczen httpx (domyslnie: 20)
CORS_ORIGINS        - lista dozwolonych origin (domyslnie: ["*"])
WORKERS             - liczba worker procesow uvicorn (domyslnie: 4)
```

### Krok 3: Crypto (`app/crypto.py`)

Zaimplementuj funkcje `encrypt_nrkrs(krs: str) -> str` wedlug algorytmu opisanego
powyzej. Uzyj `pycryptodome` (importy: `Crypto.Cipher.AES`, `Crypto.Util.Padding.pad`).

Napisz testy:
- Czy output jest prawidlowym Base64
- Czy dlugosc output jest wielokrotnoscia 24 znakow (16-byte blocks w Base64)
- Czy dwa wywolania w tej samej sekundzie daja ten sam wynik
- Czy wywolania w roznych sekundach daja rozne wyniki
- Czy KRS jest padding-owany zerami (test z "694720" i "0000694720" - ten sam plaintext prefix)

### Krok 4: RDF Client (`app/rdf_client.py`)

Async wrapper wokol `httpx.AsyncClient`.

Wymagania:
- JEDEN wspoldzielony AsyncClient (tworzony w lifespan, zamykany przy shutdown)
- Naglowki ustawione globalnie: Content-Type, Accept, User-Agent, Referer, Origin, Cache-Control, Pragma
- User-Agent: realny Chrome UA (np. "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)...")
- Timeout konfigurowalny
- Limits: max_connections z configa

Metody:
```python
async def dane_podstawowe(krs: str) -> dict
async def rodzaje_dokumentow(krs: str) -> list[dict]
async def wyszukiwanie(krs, page, page_size, sort_field, sort_dir) -> dict
async def metadata(doc_id: str) -> dict
async def download(doc_ids: list[str]) -> bytes
```

WAZNE dla `wyszukiwanie`: wywolaj `encrypt_nrkrs()` WEWNATRZ tej metody, tuz przed
wyslaniem POST-a. Nie przyjmuj zaszyfrowanego tokenu z zewnatrz.

WAZNE dla `metadata`: doc_id to Base64 ze znakami `=`, `+`, `/` - musisz URL-encode:
`urllib.parse.quote(doc_id, safe="")`.

WAZNE dla `download`: nadpisz naglowek `Accept` na `application/octet-stream`.

### Krok 5: Schemas (`app/schemas.py`)

Pydantic v2 modele. Walidacja:
- `krs`: string, regex `^\d{1,10}$`
- `page`: int >= 0
- `page_size`: int 1-100
- `sort_dir`: enum MALEJACO | ROSNACO
- `document_ids`: list[str], min 1, max 20

Modele response powinny tlumaczc polskie nazwy pol na angielskie (np. `numerKRS` -> `numer_krs`).

### Krok 6: Routers

**`app/routers/podmiot.py`** (prefix: `/api/podmiot`):
- `POST /lookup` - waliduj KRS, zwroc dane podmiotu
- `POST /document-types` - typy dokumentow dla podmiotu

**`app/routers/dokumenty.py`** (prefix: `/api/dokumenty`):
- `POST /search` - szukaj dokumentow (szyfrowanie wewnetrznie)
- `GET /metadata/{doc_id:path}` - metadane dokumentu
- `POST /download` - pobierz ZIP

Dla `/download` uzyj `StreamingResponse` z `media_type="application/zip"`.

### Krok 7: Main (`app/main.py`)

- Lifespan: `rdf_client.start()` / `rdf_client.stop()`
- CORS middleware z konfigurowalnymi origins
- Exception handler na `httpx.HTTPStatusError` -> 502 z info o upstream error
- Health check endpoint: `GET /health`

### Krok 8: Multithreading / Workers

Serwis musi obslugiwac wielu klientow rownolegle. Strategia:

1. **Uvicorn workers**: uruchom N procesow (domyslnie 4):
   ```
   uvicorn app.main:app --workers 4
   ```
   Kazdy worker to osobny proces z wlasna petla asyncio i wlasnym httpx client.

2. **Async I/O wewnatrz workera**: httpx.AsyncClient obsluguje wiele rownoleglych
   requestow w jednym procesie dzieki asyncio. Ustawienie `max_connections=20` oznacza
   ze jeden worker moze miec 20 aktywnych polaczen do upstream jednoczesnie.

3. **Sumaryczna przepustowosc**: 4 workers x 20 connections = 80 rownoleglych requestow.

4. **Dockerfile**: CMD powinno uzywac `--workers $WORKERS` (env var).

5. **NIE uzywaj threading manualne** (threading.Thread, concurrent.futures) -
   asyncio + workers to jest prawidlowe podejscie dla FastAPI.

### Krok 9: Docker

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ app/
EXPOSE 8000
ENV WORKERS=4
CMD uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers ${WORKERS}
```

`.env.example`:
```
RDF_BASE_URL=https://rdf-przegladarka.ms.gov.pl/services/rdf/przegladarka-dokumentow-finansowych
REQUEST_TIMEOUT=30
MAX_CONNECTIONS=20
CORS_ORIGINS=["http://localhost:5173"]
WORKERS=4
```

### Krok 10: Testy

Uzyj `pytest` + `pytest-asyncio` + `httpx` (as test client).

```python
# tests/test_crypto.py
- test_encrypt_produces_valid_base64()
- test_encrypt_different_seconds_different_output()
- test_encrypt_pads_krs_to_10_digits()

# tests/test_endpoints.py (z mockami httpx)
- test_lookup_found()
- test_lookup_not_found()
- test_search_documents()
- test_download_returns_zip()
- test_health_check()
```

Mockuj upstream odpowiedzi - nie bij po prawdziwym API w testach.

---

## Checklist gotowosci

- [ ] `encrypt_nrkrs()` generuje poprawny token AES-128-CBC/PKCS7 w Base64
- [ ] Wszystkie 5 endpointow upstream API jest obslugiwanych
- [ ] Doc ID z Base64 jest poprawnie URL-encoded w path parameters
- [ ] Download endpoint zwraca StreamingResponse z content-type application/zip
- [ ] CORS skonfigurowany
- [ ] httpx.AsyncClient tworzony w lifespan, zamykany przy shutdown
- [ ] Dockerfile uzywa --workers
- [ ] Testy crypto przechodza
- [ ] Testy endpointow z mockami przechodza
- [ ] Health check endpoint dziala
- [ ] Brak hardcoded secrets (token generowany dynamicznie z datetime)

---

## Czego NIE robic

- NIE cache'uj zaszyfrowanego tokenu nrKRS - generuj swiezy przy kazdym uzyciu
- NIE uzywaj `requests` (synchroniczny) - uzywaj `httpx` (async)
- NIE tworu manualnych watkow (threading) - uzywaj async + uvicorn workers
- NIE przekazuj plaintextowego KRS do endpointu wyszukiwania - musi byc zaszyfrowany
- NIE trzymaj session cookies - kazdy worker ma wlasnego httpx clienta, upstream akceptuje bezstanowe requesty
- NIE loguj pelnych tokenow AES w produkcji - loguj tylko KRS i status odpowiedzi
