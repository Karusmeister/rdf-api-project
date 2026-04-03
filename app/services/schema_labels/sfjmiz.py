"""SFJMIZ — JednostkaMikro (Annex 4 of the Accounting Act)."""

from app.services.schema_labels import SectionConfig, SchemaConfig, register_schema

TAG_LABELS: dict[str, str] = {
    # === BILANS - AKTYWA (BilansJednostkaMikro) ===
    "Aktywa": "AKTYWA",
    "Aktywa_A": "A. Aktywa trwale, w tym srodki trwale",
    "Aktywa_B": "B. Aktywa obrotowe, w tym:",
    "Aktywa_B_1": "1. zapasy",
    "Aktywa_B_2": "2. naleznosci krotkoterminowe",
    "Aktywa_C": "C. Nalezne wplaty na kapital (fundusz) podstawowy",
    "Aktywa_D": "D. Udzialy (akcje) wlasne",

    # === BILANS - PASYWA ===
    "Pasywa": "PASYWA",
    "Pasywa_A": "A. Kapital (fundusz) wlasny, w tym:",
    "Pasywa_A_1": "1. kapital (fundusz) podstawowy",
    "Pasywa_B": "B. Zobowiazania i rezerwy na zobowiazania, w tym:",
    "Pasywa_B_1": "1. rezerwy na zobowiazania",
    "Pasywa_B_2": "2. zobowiazania z tytulu kredytow i pozyczek",

    # === RZiS (RZiSJednostkaMikro — direct children, no variant wrapper) ===
    "A": "A. Przychody z podstawowej dzialalnosci operacyjnej",
    "A_1": "(w tym: zmiana stanu produktow)",
    "B": "B. Koszty podstawowej dzialalnosci operacyjnej",
    "B_I": "I. Amortyzacja",
    "B_II": "II. Zuzycie materialow i energii",
    "B_III": "III. Wynagrodzenia, ubezpieczenia spoleczne i inne swiadczenia",
    "B_IV": "IV. Pozostale koszty",
    "C": "C. Pozostale przychody i zyski, w tym aktualizacja wartosci aktywow",
    "C_1": "(w tym: aktualizacja wartosci aktywow)",
    "D": "D. Pozostale koszty i straty, w tym aktualizacja wartosci aktywow",
    "D_1": "(w tym: aktualizacja wartosci aktywow)",
    "E": "E. Podatek dochodowy",
    "F": "F. Zysk/strata netto (A-B+C-D-E)",
    "G": "G. Laczne obowiazkowe zmniejszenia zysku (zwiekszenia straty)",
}

SECTIONS: SectionConfig = {
    "bilans_element": "BilansJednostkaMikro",
    "aktywa_element": "Aktywa",
    "pasywa_element": "Pasywa",
    "rzis_element": "RZiSJednostkaMikro",
    "rzis_has_variants": False,
    "cf_element": None,
    "cf_has_variants": False,
    "unit_multiplier": 1,
    "extra_sections": [],
}

CONFIG: SchemaConfig = {
    "code": "SFJMIZ",
    "root_tags": [
        "JednostkaMikro",
        "SprFinJednostkaMikroWZlotych",
        "JednostkaMikroWTys",
        "SprFinJednostkaMikroWTysiacach",
    ],
    "sections": SECTIONS,
    "tag_labels": TAG_LABELS,
    "statement_markers": ["BilansJednostkaMikro", "RZiSJednostkaMikro"],
}

register_schema(CONFIG)
