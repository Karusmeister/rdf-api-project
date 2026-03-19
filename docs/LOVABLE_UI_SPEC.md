# RDF Browser - Lovable UI Specification

> Prosty interfejs webowy do przegladania i pobierania dokumentow finansowych
> z Repozytorium Dokumentow Finansowych (RDF) Ministerstwa Sprawiedliwosci.
>
> Frontend: Lovable (React + Tailwind + shadcn/ui)
> Backend: FastAPI proxy at configurable `VITE_API_URL`

---

## Overview

Aplikacja single-page z trzema glownymi widokami/stanami:

```
[Wyszukiwarka KRS] --> [Lista dokumentow] --> [Podglad / Pobieranie]
```

Uzytkownik wpisuje numer KRS, widzi liste dokumentow podmiotu, moze je filtrowac
i pobierac pliki XML/ZIP.

---

## Global Layout

- Naglowek: logo/nazwa "RDF Browser", prosta nawigacja (tylko powrot do wyszukiwarki)
- Tresc glowna: centrycznie, max-width 960px
- Stopka: informacja "Dane z rdf-przegladarka.ms.gov.pl - uzycie nieoficjalnego API"
- Motyw: jasny, minimalistyczny, shadcn/ui default theme

---

## View 1: Wyszukiwarka KRS (strona glowna)

### Layout

Centrowany formularz na srodku strony (vertically centered na duzych ekranach).

### Elementy

1. **Naglowek**: "Przegladarka Dokumentow Finansowych"
2. **Pole tekstowe KRS**:
   - Label: "Numer KRS"
   - Placeholder: "np. 0000694720"
   - Walidacja: tylko cyfry, max 10 znakow
   - Auto-padding zerami z lewej do 10 cyfr (wizualny hint pod polem)
3. **Przycisk "Szukaj"**: primary button, disabled gdy pole puste lub nieprawidlowe
4. **Loading state**: spinner + "Weryfikacja podmiotu..." po kliknieciu

### Logika

1. User wpisuje KRS, klika "Szukaj"
2. POST `/api/podmiot/lookup` z `{ krs: "0000694720" }`
3. Jesli `found: true` - przejscie do View 2 z danymi podmiotu
4. Jesli `found: false` - komunikat bledu inline pod polem:
   "Nie znaleziono podmiotu o numerze KRS XXXXXXXXXX"
5. Jesli blad sieci - toast z komunikatem bledu

### Stan globalny do przekazania dalej

```typescript
interface AppState {
  krs: string;              // "0000694720"
  podmiot: {
    nazwa: string;          // "FIRMA SP. Z O.O."
    formaPrawna: string;    // "SPOLKA Z OGRANICZONA..."
    wykreslony: boolean;
  };
}
```

---

## View 2: Lista dokumentow

### Layout

Gora: karta z informacjami o podmiocie.
Ponizej: tabela dokumentow z filtrami i paginacja.

### Elementy

1. **Karta podmiotu** (shadcn Card):
   - Nazwa podmiotu (bold, duzy font)
   - KRS: 0000694720
   - Forma prawna: Spolka z o.o.
   - Badge "Wykreslony" jesli wykreslony == true (czerwony badge)
   - Przycisk "Zmien podmiot" (ghost button, wraca do View 1)

2. **Pasek filtrow** (nad tabela):
   - Select "Typ dokumentu" z opcjami:
     - Wszystkie (domyslnie)
     - Roczne sprawozdanie finansowe (rodzaj: "18")
     - Uchwala o podziale zysku (rodzaj: "4")
     - Uchwala o zatwierdzeniu (rodzaj: "3")
     - Sprawozdanie z dzialalnosci (rodzaj: "19")
     - Opinia bieglego rewidenta (rodzaj: "20")
   - Select "Sortowanie": Najnowsze / Najstarsze

3. **Tabela dokumentow** (shadcn Table):

   | Kolumna | Zrodlo | Opis |
   |---------|--------|------|
   | Typ | `rodzaj` (mapowany na nazwe) | Ikona + nazwa typu |
   | Okres | `okres_od` - `okres_do` | np. "2023-01-01 - 2023-12-31" |
   | Status | `status` | Badge: zielony "Aktywny" / szary "Usuniety" |
   | Akcje | - | Przycisk "Szczegoly" + przycisk "Pobierz" |

4. **Paginacja** (pod tabela):
   - Przyciski Poprzednia / Nastepna
   - Info: "Strona X z Y (Z dokumentow)"

### Logika

1. Przy wejsciu: POST `/api/dokumenty/search` z `{ krs, page: 0, page_size: 10 }`
2. Zmiana filtru typu: dodaj `rodzaj` do requestu, reset na page 0
3. Zmiana sortowania: zmien `sort_dir`, reset na page 0
4. Klikniecie "Szczegoly" -> przejscie do View 3
5. Klikniecie "Pobierz" -> POST `/api/dokumenty/download` z `{ document_ids: [id] }`
   - Pobierz blob, stworz link do pobrania, trigger download w przegladarce
   - Nazwa pliku: `KRS_{krs}_{okres_do}_{rodzaj}.zip`

### Mapowanie kodow na nazwy

```typescript
const RODZAJ_MAP: Record<string, string> = {
  "3": "Uchwala o zatwierdzeniu sprawozdania",
  "4": "Uchwala o podziale zysku / pokryciu straty",
  "18": "Roczne sprawozdanie finansowe",
  "19": "Sprawozdanie z dzialalnosci",
  "20": "Opinia bieglego rewidenta",
};
```

---

## View 3: Szczegoly dokumentu

### Layout

Pelna strona metadanych z przyciskiem pobierania.

### Elementy

1. **Breadcrumb**: Wyszukiwarka > {nazwa podmiotu} > Dokument
2. **Karta metadanych** (shadcn Card z sekcjami):
   - Typ dokumentu (pelna nazwa)
   - Numer ID dokumentu
   - Standard: badge "UoR" (niebieski) lub "MSR/MSSF" (fioletowy)
     - Na podstawie pola `czy_msr`
   - Korekta: badge "Tak" / "Nie"
   - Data sporzadzenia
   - Data dodania do RDF
   - Okres sprawozdawczy: od - do
   - Jezyk
   - Nazwa pliku oryginalnego

3. **Przycisk "Pobierz dokument"**: duzy primary button
   - Loading state: "Pobieranie..."
   - Po pobraniu: automatyczny download pliku ZIP

4. **Przycisk "Powrot do listy"**: secondary button

### Logika

1. Przy wejsciu: GET `/api/dokumenty/metadata/{doc_id}`
2. Pobieranie: POST `/api/dokumenty/download` z `{ document_ids: [doc_id] }`

---

## API Client (src/lib/api.ts)

```typescript
const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

export const api = {
  lookupEntity: (krs: string) =>
    fetch(`${API_URL}/api/podmiot/lookup`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ krs }),
    }).then(r => r.json()),

  searchDocuments: (params: {
    krs: string;
    page?: number;
    page_size?: number;
    sort_dir?: string;
    rodzaj?: string;
  }) =>
    fetch(`${API_URL}/api/dokumenty/search`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    }).then(r => r.json()),

  getMetadata: (docId: string) =>
    fetch(`${API_URL}/api/dokumenty/metadata/${encodeURIComponent(docId)}`)
      .then(r => r.json()),

  downloadDocument: async (docIds: string[]) => {
    const resp = await fetch(`${API_URL}/api/dokumenty/download`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ document_ids: docIds }),
    });
    return resp.blob();
  },
};
```

---

## Routing (react-router)

```
/                   -> View 1 (Wyszukiwarka)
/podmiot/:krs       -> View 2 (Lista dokumentow)
/dokument/:docId    -> View 3 (Szczegoly)
```

Stan podmiotu (nazwa, forma prawna) trzymany w React context lub URL state.
Jesli user wejdzie bezposrednio na `/podmiot/:krs`, wykonaj lookup automatycznie.

---

## Error States

| Scenariusz | Zachowanie |
|------------|-----------|
| KRS nie znaleziony | Inline error pod polem wyszukiwania |
| Backend niedostepny | Toast: "Serwer niedostepny - sprobuj pozniej" |
| Timeout | Toast: "Zapytanie trwa zbyt dlugo - sprobuj ponownie" |
| Brak dokumentow | Empty state w tabeli: "Brak dokumentow dla tego podmiotu" |
| Download error | Toast: "Nie udalo sie pobrac dokumentu" |

---

## Environment Variables

```
VITE_API_URL=http://localhost:8000
```

---

## Uwagi dla Lovable

- Uzyj shadcn/ui komponentow: Card, Table, Button, Input, Select, Badge, Toast, Skeleton
- Stan aplikacji: React context lub zustand (proste - 3 widoki)
- Responsywnosc: tabela w View 2 powinna byc scrollowalna na mobile
- Nie implementuj logiki szyfrowania KRS po stronie frontendu - backend to obsluguje
- Wszystkie komunikaty UI po polsku
