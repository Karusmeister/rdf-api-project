"""
XML parser for Polish GAAP financial statements (JednostkaInna schema).

Flow: ZIP bytes → extract XML → strip namespaces → parse tree → structured dict
"""

import io
import re
import time
import zipfile
import xml.etree.ElementTree as ET
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Tag-to-label dictionary (JednostkaInna schema, Zalacznik nr 1)
# Keys are stored tag names (Bilans use raw XML tags; RZiS stored as "RZiS.X";
# CF stored as "CF.X"). Label lookup falls back to raw tag name.
# ---------------------------------------------------------------------------

TAG_LABELS: dict[str, str] = {
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
    "Pasywa_A_II": "II. Kapital (fundusz) zapasowy, w tym nadwyzka wartosci sprzedazy nad wartoscia nominalna udzialow (akcji)",
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

    # === RZiS POROWNAWCZY (raw XML tag names — stored with "RZiS." prefix in trees) ===
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
    "CF.C_I_1": "1. Wplywy netto z wydania udzialow i innych instrumentow kapitalowych oraz doplat do kapitalu",
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

# ---------------------------------------------------------------------------
# Simple in-memory cache
# ---------------------------------------------------------------------------

_cache: dict[str, tuple[float, Any]] = {}
CACHE_TTL = 3600.0
CACHE_MAX_SIZE = 512


def cache_get(key: str) -> Any:
    entry = _cache.get(key)
    if entry is None:
        return None
    if (time.time() - entry[0]) >= CACHE_TTL:
        del _cache[key]
        return None
    return entry[1]


def cache_set(key: str, value: Any) -> None:
    now = time.time()
    # Evict all expired entries first
    expired = [k for k, (ts, _) in _cache.items() if now - ts >= CACHE_TTL]
    for k in expired:
        del _cache[k]
    # If still at capacity, evict the oldest entries
    if len(_cache) >= CACHE_MAX_SIZE:
        oldest = sorted(_cache.keys(), key=lambda k: _cache[k][0])
        for k in oldest[: len(_cache) - CACHE_MAX_SIZE + 1]:
            del _cache[k]
    _cache[key] = (now, value)


# ---------------------------------------------------------------------------
# ZIP / XML helpers
# ---------------------------------------------------------------------------

_STATEMENT_MARKERS = frozenset({"Bilans", "RZiS", "RachPrzeplywow"})


def extract_xml_from_zip(zip_bytes: bytes) -> str:
    """
    Return the financial statement XML from a ZIP archive.
    Prefers any XML whose parsed tree contains Bilans, RZiS, or RachPrzeplywow
    over other XML files (e.g. digital signatures) that may be in the archive.
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        xml_names = sorted(n for n in zf.namelist() if n.lower().endswith(".xml"))
        if not xml_names:
            raise ValueError("No XML file found in ZIP")

        first_content: Optional[str] = None
        for name in xml_names:
            content = zf.read(name).decode("utf-8", errors="replace")
            if first_content is None:
                first_content = content
            try:
                root = ET.fromstring(content)
                # Strip namespaces from tag names for the marker check
                def _local(tag: str) -> str:
                    return tag.split("}", 1)[1] if "}" in tag else tag
                if any(
                    _local(el.tag) in _STATEMENT_MARKERS
                    for el in root.iter()
                ):
                    return content
            except ET.ParseError:
                continue

        raise ValueError(f"No parseable financial statement XML found in ZIP (files: {xml_names})")


def parse_xml_no_ns(xml_string: str) -> ET.Element:
    """
    Parse XML and strip all namespace URIs from tag and attribute names.
    More robust than text-level regex stripping because the parser handles
    namespace resolution; we then just remove the {uri} prefixes.
    """
    root = ET.fromstring(xml_string)
    for elem in root.iter():
        if "}" in elem.tag:
            elem.tag = elem.tag.split("}", 1)[1]
        new_attrib = {}
        for k, v in elem.attrib.items():
            if "}" in k:
                k = k.split("}", 1)[1]
            new_attrib[k] = v
        elem.attrib = new_attrib
    return root


# ---------------------------------------------------------------------------
# Tree extraction
# ---------------------------------------------------------------------------

def _get_label(tag: str, raw_tag: str) -> str:
    return TAG_LABELS.get(tag) or TAG_LABELS.get(raw_tag) or tag


def _is_w_tym(tag: str, raw_tag: str) -> bool:
    label = _get_label(tag, raw_tag)
    return label.startswith("(")


def _parse_float(element: ET.Element, child_tag: str) -> float:
    el = element.find(child_tag)
    if el is not None and el.text:
        try:
            return float(el.text.strip().replace(",", "."))
        except ValueError:
            return 0.0
    return 0.0


def extract_tree(element: ET.Element, depth: int = 0, tag_prefix: str = "") -> Optional[dict]:
    """
    Recursively build a FinancialNode dict from an XML element.
    Only nodes with a KwotaA child are included.
    tag_prefix: "RZiS." for income statement nodes, "CF." for cash flow nodes.
    """
    if element.find("KwotaA") is None:
        return None

    raw_tag = element.tag
    tag = f"{tag_prefix}{raw_tag}" if tag_prefix else raw_tag

    kwota_a = _parse_float(element, "KwotaA")
    kwota_b = _parse_float(element, "KwotaB")
    kwota_b1_el = element.find("KwotaB1")
    kwota_b1: Optional[float] = None
    if kwota_b1_el is not None and kwota_b1_el.text:
        try:
            kwota_b1 = float(kwota_b1_el.text.strip().replace(",", "."))
        except ValueError:
            pass

    children = []
    for child in element:
        if child.tag in ("KwotaA", "KwotaB", "KwotaB1"):
            continue
        child_node = extract_tree(child, depth + 1, tag_prefix)
        if child_node is not None:
            children.append(child_node)

    return {
        "tag": tag,
        "label": _get_label(tag, raw_tag),
        "kwota_a": kwota_a,
        "kwota_b": kwota_b,
        "kwota_b1": kwota_b1,
        "depth": depth,
        "is_w_tym": _is_w_tym(tag, raw_tag),
        "children": children,
    }


# ---------------------------------------------------------------------------
# Company metadata extraction
# ---------------------------------------------------------------------------

def _extract_company_info(root: ET.Element) -> dict:
    def text(path: str) -> Optional[str]:
        el = root.find(path)
        return el.text.strip() if el is not None and el.text else None

    return {
        "name": text(".//NazwaFirmy"),
        "krs": text(".//P_1E"),
        "nip": text(".//P_1D"),
        "pkd": text(".//KodPKD"),
        "period_start": text(".//P_3/DataOd") or text(".//OkresOd"),
        "period_end": text(".//P_3/DataDo") or text(".//OkresDo"),
        "date_prepared": text(".//DataSporzadzenia"),
    }


# ---------------------------------------------------------------------------
# Full statement parsing
# ---------------------------------------------------------------------------

def parse_statement(xml_string: str) -> dict:
    """Parse a financial statement XML string into a structured dict."""
    root = parse_xml_no_ns(xml_string)

    company = _extract_company_info(root)
    company["schema_type"] = root.tag

    # Bilans
    aktywa_node: Optional[dict] = None
    pasywa_node: Optional[dict] = None
    bilans_el = root.find(".//Bilans")
    if bilans_el is not None:
        a_el = bilans_el.find("Aktywa")
        p_el = bilans_el.find("Pasywa")
        if a_el is not None:
            aktywa_node = extract_tree(a_el, depth=0)
        if p_el is not None:
            pasywa_node = extract_tree(p_el, depth=0)

    # RZiS
    rzis_nodes: list[dict] = []
    rzis_variant: Optional[str] = None
    rzis_el = root.find(".//RZiS")
    if rzis_el is not None:
        rzis_por = rzis_el.find("RZiSPor")
        rzis_kal = rzis_el.find("RZiSKal")
        rzis_content = rzis_por or rzis_kal
        rzis_variant = (
            "porownawczy" if rzis_por is not None
            else "kalkulacyjny" if rzis_kal is not None
            else None
        )
        if rzis_content is not None:
            for child in rzis_content:
                node = extract_tree(child, depth=0, tag_prefix="RZiS.")
                if node is not None:
                    rzis_nodes.append(node)

    # Cash flow
    cf_nodes: list[dict] = []
    cf_method: Optional[str] = None
    rach_el = root.find(".//RachPrzeplywow")
    if rach_el is not None:
        cf_posr = rach_el.find("PrzeplywyPosr")
        cf_bezp = rach_el.find("PrzeplywyBezp")
        cf_content = cf_posr or cf_bezp
        cf_method = (
            "posrednia" if cf_posr is not None
            else "bezposrednia" if cf_bezp is not None
            else None
        )
        if cf_content is not None:
            for child in cf_content:
                node = extract_tree(child, depth=0, tag_prefix="CF.")
                if node is not None:
                    cf_nodes.append(node)

    company["rzis_variant"] = rzis_variant
    company["cf_method"] = cf_method

    return {
        "company": company,
        "bilans": {"aktywa": aktywa_node, "pasywa": pasywa_node},
        "rzis": rzis_nodes,
        "cash_flow": cf_nodes,
    }


# ---------------------------------------------------------------------------
# Tree lookup helpers
# ---------------------------------------------------------------------------

def find_node_value(
    tree: Any,
    tag: str,
    kwota: str = "kwota_a",
) -> Optional[float]:
    """Recursively find a node by tag and return its kwota value."""
    if tree is None:
        return None
    if isinstance(tree, dict):
        if tree.get("tag") == tag:
            return tree.get(kwota, 0.0)
        for child in tree.get("children", []):
            result = find_node_value(child, tag, kwota)
            if result is not None:
                return result
    elif isinstance(tree, list):
        for item in tree:
            result = find_node_value(item, tag, kwota)
            if result is not None:
                return result
    return None


def find_value(stmt: dict, tag: str, kwota: str = "kwota_a") -> Optional[float]:
    """Search across bilans + rzis + cash_flow."""
    for section in (stmt["bilans"]["aktywa"], stmt["bilans"]["pasywa"]):
        v = find_node_value(section, tag, kwota)
        if v is not None:
            return v
    v = find_node_value(stmt["rzis"], tag, kwota)
    if v is not None:
        return v
    return find_node_value(stmt["cash_flow"], tag, kwota)


def extract_flat_values(stmt: dict, use_kwota_b: bool = False) -> dict[str, Optional[float]]:
    """
    Flatten all nodes in a parsed statement to {tag: value}.
    use_kwota_b=True extracts the embedded prior-year column.
    """
    kwota_key = "kwota_b" if use_kwota_b else "kwota_a"
    result: dict[str, Optional[float]] = {}

    def _traverse(node: Any) -> None:
        if isinstance(node, dict):
            if "tag" in node:
                result[node["tag"]] = node.get(kwota_key)
            for child in node.get("children", []):
                _traverse(child)
        elif isinstance(node, list):
            for item in node:
                _traverse(item)

    _traverse(stmt["bilans"]["aktywa"])
    _traverse(stmt["bilans"]["pasywa"])
    _traverse(stmt["rzis"])
    _traverse(stmt["cash_flow"])
    return result


# ---------------------------------------------------------------------------
# Comparison tree building
# ---------------------------------------------------------------------------

def _safe_pct(current: float, previous: float) -> Optional[float]:
    if previous == 0:
        return None
    return round((current - previous) / abs(previous) * 100, 2)


def _safe_share(value: float, parent: Optional[float]) -> Optional[float]:
    if parent is None or parent == 0:
        return None
    return round(value / parent * 100, 4)


def node_to_comparison(
    node: dict,
    parent_current: Optional[float] = None,
    parent_previous: Optional[float] = None,
    previous_node: Optional[dict] = None,
    use_kwota_b_fallback: bool = True,
) -> dict:
    """
    Convert a FinancialNode to a ComparisonNode.
    If previous_node is provided, uses its kwota_a as the previous value.
    If use_kwota_b_fallback is True and previous_node is absent, falls back to
    node.kwota_b (embedded prior-year column) — correct only for single-statement mode.
    If use_kwota_b_fallback is False and previous_node is absent, previous is None
    and all derived change/share fields are also None — correct for two-statement mode.
    """
    current = node.get("kwota_a", 0.0)
    if previous_node is not None:
        previous: Optional[float] = previous_node.get("kwota_a", 0.0)
    elif use_kwota_b_fallback:
        previous = node.get("kwota_b", 0.0)
    else:
        previous = None

    prev_children_map: dict[str, dict] = {}
    if previous_node:
        for c in previous_node.get("children", []):
            prev_children_map[c["tag"]] = c

    children = []
    for child in node.get("children", []):
        prev_child = prev_children_map.get(child["tag"]) if previous_node else None
        children.append(
            node_to_comparison(child, current, previous, prev_child, use_kwota_b_fallback)
        )

    if previous is not None:
        change_absolute: Optional[float] = round(current - previous, 2)
        change_percent = _safe_pct(current, previous)
        share_prev = _safe_share(previous, parent_previous)
    else:
        change_absolute = None
        change_percent = None
        share_prev = None

    return {
        "tag": node["tag"],
        "label": node["label"],
        "current": current,
        "previous": previous,
        "change_absolute": change_absolute,
        "change_percent": change_percent,
        "share_of_parent_current": _safe_share(current, parent_current),
        "share_of_parent_previous": share_prev,
        "depth": node["depth"],
        "is_w_tym": node["is_w_tym"],
        "children": children,
    }


def build_comparison(
    current_stmt: dict,
    previous_stmt: Optional[dict] = None,
) -> dict:
    """
    Build a full comparison structure from one or two parsed statements.
    If previous_stmt is None, uses kwota_b (embedded column) from current_stmt.
    If previous_stmt is provided, only that statement's kwota_a is used as the
    previous value — kwota_b from current_stmt is never read.
    """
    # Use kwota_b fallback only in single-statement mode
    use_kwota_b = previous_stmt is None

    def _find_node(stmt: dict, tag: str) -> Optional[dict]:
        for section in (stmt["bilans"]["aktywa"], stmt["bilans"]["pasywa"]):
            result = _find_in_tree(section, tag)
            if result:
                return result
        for item in stmt["rzis"]:
            result = _find_in_tree(item, tag)
            if result:
                return result
        for item in stmt["cash_flow"]:
            result = _find_in_tree(item, tag)
            if result:
                return result
        return None

    def _find_in_tree(node: Optional[dict], tag: str) -> Optional[dict]:
        if node is None:
            return None
        if node.get("tag") == tag:
            return node
        for child in node.get("children", []):
            result = _find_in_tree(child, tag)
            if result:
                return result
        return None

    def cmp_tree(node: Optional[dict]) -> Optional[dict]:
        if node is None:
            return None
        prev_node = None
        if previous_stmt is not None:
            prev_node = _find_node(previous_stmt, node["tag"])
        return node_to_comparison(node, None, None, prev_node, use_kwota_b)

    def cmp_list(nodes: list[dict]) -> list[dict]:
        result = []
        for node in nodes:
            prev_node = None
            if previous_stmt is not None:
                prev_node = _find_node(previous_stmt, node["tag"])
            result.append(node_to_comparison(node, None, None, prev_node, use_kwota_b))
        return result

    return {
        "bilans": {
            "aktywa": cmp_tree(current_stmt["bilans"]["aktywa"]),
            "pasywa": cmp_tree(current_stmt["bilans"]["pasywa"]),
        },
        "rzis": cmp_list(current_stmt["rzis"]),
        "cash_flow": cmp_list(current_stmt["cash_flow"]),
    }


# ---------------------------------------------------------------------------
# Financial ratios
# ---------------------------------------------------------------------------

def compute_ratios(stmt: dict, use_kwota_b: bool = False) -> dict:
    """Compute key financial ratios from a parsed statement."""
    kwota = "kwota_b" if use_kwota_b else "kwota_a"

    def v(tag: str) -> Optional[float]:
        return find_value(stmt, tag, kwota)

    def ratio(num: Optional[float], den: Optional[float]) -> Optional[float]:
        if num is None or den is None or den == 0:
            return None
        return round(num / den, 4)

    def pct_change(a: Optional[float], b: Optional[float]) -> Optional[float]:
        if a is None or b is None or b == 0:
            return None
        return round((a - b) / abs(b) * 100, 2)

    aktywa = v("Aktywa")
    pasywa_a = v("Pasywa_A")
    aktywa_b = v("Aktywa_B")
    pasywa_b = v("Pasywa_B")
    pasywa_b_iii = v("Pasywa_B_III")
    rzis_a = v("RZiS.A")
    rzis_f = v("RZiS.F")
    rzis_l = v("RZiS.L")

    return {
        "equity_ratio": ratio(pasywa_a, aktywa),
        "current_ratio": ratio(aktywa_b, pasywa_b_iii),
        "debt_ratio": ratio(pasywa_b, aktywa),
        "operating_margin": ratio(rzis_f, rzis_a),
        "net_margin": ratio(rzis_l, rzis_a),
    }
