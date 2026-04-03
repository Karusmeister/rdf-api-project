"""SFJOPZ — JednostkaOp / Organizacja Pozytku Publicznego (Annex 6 of the Accounting Act)."""

from app.services.schema_labels import SectionConfig, SchemaConfig, register_schema

TAG_LABELS: dict[str, str] = {
    # === BILANS - AKTYWA (BilansJednostkaOp) ===
    "Aktywa": "AKTYWA",
    "Aktywa_A": "A. Aktywa trwale",
    "Aktywa_A_I": "I. Wartosci niematerialne i prawne",
    "Aktywa_A_II": "II. Rzeczowe aktywa trwale",
    "Aktywa_A_III": "III. Naleznosci dlugoterminowe",
    "Aktywa_A_IV": "IV. Inwestycje dlugoterminowe",
    "Aktywa_A_V": "V. Dlugoterminowe rozliczenia miedzyokresowe",
    "Aktywa_B": "B. Aktywa obrotowe",
    "Aktywa_B_I": "I. Zapasy",
    "Aktywa_B_II": "II. Naleznosci krotkoterminowe",
    "Aktywa_B_III": "III. Inwestycje krotkoterminowe",
    "Aktywa_B_IV": "IV. Krotkoterminowe rozliczenia miedzyokresowe",
    "Aktywa_C": "C. Nalezne wplaty na fundusz statutowy",

    # === BILANS - PASYWA ===
    "Pasywa": "PASYWA",
    "Pasywa_A": "A. Fundusz wlasny",
    "Pasywa_A_I": "I. Fundusz statutowy",
    "Pasywa_A_II": "II. Fundusz z aktualizacji wyceny",
    "Pasywa_A_III": "III. Wynik finansowy netto za rok obrotowy",
    "Pasywa_A_IV": "IV. Wynik finansowy z lat ubieglych",
    "Pasywa_B": "B. Zobowiazania i rezerwy na zobowiazania",
    "Pasywa_B_I": "I. Rezerwy na zobowiazania",
    "Pasywa_B_II": "II. Zobowiazania dlugoterminowe",
    "Pasywa_B_III": "III. Zobowiazania krotkoterminowe",
    "Pasywa_B_IV": "IV. Rozliczenia miedzyokresowe",

    # === RZiS (RZiSJednostkaOp — direct children, no variant wrapper) ===
    "A": "A. Przychody z dzialalnosci statutowej",
    "A_I": "I. Przychody z dzialalnosci nieodplatnej pozytku publicznego",
    "A_II": "II. Przychody z dzialalnosci odplatnej pozytku publicznego",
    "A_III": "III. Przychody z pozostalej dzialalnosci statutowej",
    "B": "B. Koszty dzialalnosci statutowej",
    "B_I": "I. Koszty dzialalnosci nieodplatnej pozytku publicznego",
    "B_II": "II. Koszty dzialalnosci odplatnej pozytku publicznego",
    "B_III": "III. Koszty pozostalej dzialalnosci statutowej",
    "C": "C. Zysk (strata) z dzialalnosci statutowej (A-B)",
    "D": "D. Przychody z dzialalnosci gospodarczej",
    "E": "E. Koszty dzialalnosci gospodarczej",
    "F": "F. Zysk (strata) z dzialalnosci gospodarczej (D-E)",
    "G": "G. Koszty ogolnego zarzadu",
    "H": "H. Zysk (strata) z dzialalnosci operacyjnej (C+F-G)",
    "I": "I. Pozostale przychody operacyjne",
    "J": "J. Pozostale koszty operacyjne",
    "K": "K. Przychody finansowe",
    "L": "L. Koszty finansowe",
    "M": "M. Zysk (strata) brutto (H+I-J+K-L)",
    "N": "N. Podatek dochodowy",
    "O": "O. Zysk (strata) netto (M-N)",
}

SECTIONS: SectionConfig = {
    "bilans_element": "BilansJednostkaOp",
    "aktywa_element": "Aktywa",
    "pasywa_element": "Pasywa",
    "rzis_element": "RZiSJednostkaOp",
    "rzis_has_variants": False,
    "cf_element": None,
    "cf_has_variants": False,
    "unit_multiplier": 1,
    "extra_sections": [],
}

CONFIG: SchemaConfig = {
    "code": "SFJOPZ",
    "root_tags": [
        "JednostkaOp",
        "SprFinJednostkaOPPWZlotych",
        "JednostkaOpWTys",
        "SprFinJednostkaOPPWTysiacach",
        "JednostkaOrganizacjiPozarzadowej",
    ],
    "sections": SECTIONS,
    "tag_labels": TAG_LABELS,
    "statement_markers": ["BilansJednostkaOp", "RZiSJednostkaOp"],
}

register_schema(CONFIG)
