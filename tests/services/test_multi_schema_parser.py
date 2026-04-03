"""
Tests for multi-schema XML financial statement parsing.

Verifies that the parser correctly detects and dispatches to each of the
five supported schema types: SFJINZ, SFJMAZ, SFJMIZ, SFJOPZ, SFZURT.
"""

import pytest

from app.services import xml_parser
from app.services.schema_labels import SCHEMA_REGISTRY, detect_schema


# ---------------------------------------------------------------------------
# Registry and detection tests
# ---------------------------------------------------------------------------

class TestSchemaRegistry:
    def test_all_five_schemas_registered(self):
        assert set(SCHEMA_REGISTRY.keys()) == {"SFJINZ", "SFJMAZ", "SFJMIZ", "SFJOPZ", "SFZURT"}

    @pytest.mark.parametrize("root_tag,expected_code", [
        ("JednostkaInna", "SFJINZ"),
        ("SprFinJednostkaInnaWZlotych", "SFJINZ"),
        ("SprFinJednostkaInnaWTysiacach", "SFJINZ"),
        ("JednostkaMala", "SFJMAZ"),
        ("SprFinJednostkaMalaWZlotych", "SFJMAZ"),
        ("SprFinJednostkaMalaWTysiacach", "SFJMAZ"),
        ("JednostkaMikro", "SFJMIZ"),
        ("SprFinJednostkaMikroWZlotych", "SFJMIZ"),
        ("SprFinJednostkaMikroWTysiacach", "SFJMIZ"),
        ("JednostkaOp", "SFJOPZ"),
        ("SprFinJednostkaOPPWZlotych", "SFJOPZ"),
        ("SprFinJednostkaOPPWTysiacach", "SFJOPZ"),
        ("ZakladUbezpieczenWTys", "SFZURT"),
        ("SprFinZakladUbezpieczenWTysiacach", "SFZURT"),
        ("SprFinZakladUbezpieczenWZlotych", "SFZURT"),
    ])
    def test_detect_schema(self, root_tag, expected_code):
        schema = detect_schema(root_tag)
        assert schema is not None
        assert schema["code"] == expected_code

    def test_detect_unknown_returns_none(self):
        assert detect_schema("SomeUnknownRoot") is None


# ---------------------------------------------------------------------------
# Minimal XML fixtures for each schema
# ---------------------------------------------------------------------------

SFJINZ_XML = """<?xml version="1.0"?>
<JednostkaInna>
  <Naglowek><KodSprawozdania kodSystemowy="SFJINZ (1)" wersjaSchemy="1-2"/></Naglowek>
  <WprowadzenieDoSprawozdaniaFinansowego>
    <P_1><NazwaFirmy>Test SA</NazwaFirmy></P_1>
    <P_1D>1234567890</P_1D>
    <P_1E>0000012345</P_1E>
    <P_3><DataOd>2023-01-01</DataOd><DataDo>2023-12-31</DataDo></P_3>
    <DataSporzadzenia>2024-03-15</DataSporzadzenia>
  </WprowadzenieDoSprawozdaniaFinansowego>
  <Bilans>
    <Aktywa><KwotaA>1000.00</KwotaA><KwotaB>900.00</KwotaB>
      <Aktywa_A><KwotaA>600.00</KwotaA><KwotaB>500.00</KwotaB></Aktywa_A>
      <Aktywa_B><KwotaA>400.00</KwotaA><KwotaB>400.00</KwotaB></Aktywa_B>
    </Aktywa>
    <Pasywa><KwotaA>1000.00</KwotaA><KwotaB>900.00</KwotaB>
      <Pasywa_A><KwotaA>500.00</KwotaA><KwotaB>450.00</KwotaB></Pasywa_A>
      <Pasywa_B><KwotaA>500.00</KwotaA><KwotaB>450.00</KwotaB></Pasywa_B>
    </Pasywa>
  </Bilans>
  <RZiS>
    <RZiSPor>
      <A><KwotaA>2000.00</KwotaA><KwotaB>1800.00</KwotaB></A>
      <B><KwotaA>1500.00</KwotaA><KwotaB>1400.00</KwotaB></B>
      <L><KwotaA>100.00</KwotaA><KwotaB>80.00</KwotaB></L>
    </RZiSPor>
  </RZiS>
  <RachPrzeplywow>
    <PrzeplywyPosr>
      <A_I><KwotaA>100.00</KwotaA><KwotaB>80.00</KwotaB></A_I>
    </PrzeplywyPosr>
  </RachPrzeplywow>
</JednostkaInna>
"""

SFJMAZ_XML = """<?xml version="1.0"?>
<JednostkaMala>
  <WprowadzenieDoSprawozdaniaFinansowegoJednostkaMala>
    <P_1><NazwaFirmy>Mala Sp. z o.o.</NazwaFirmy></P_1>
    <P_1D>9876543210</P_1D>
    <P_1E>0000067890</P_1E>
    <P_3><DataOd>2023-01-01</DataOd><DataDo>2023-12-31</DataDo></P_3>
    <DataSporzadzenia>2024-04-01</DataSporzadzenia>
  </WprowadzenieDoSprawozdaniaFinansowegoJednostkaMala>
  <BilansJednostkaMala>
    <Aktywa><KwotaA>500.00</KwotaA><KwotaB>400.00</KwotaB>
      <Aktywa_A><KwotaA>200.00</KwotaA><KwotaB>180.00</KwotaB></Aktywa_A>
      <Aktywa_B><KwotaA>300.00</KwotaA><KwotaB>220.00</KwotaB></Aktywa_B>
    </Aktywa>
    <Pasywa><KwotaA>500.00</KwotaA><KwotaB>400.00</KwotaB>
      <Pasywa_A><KwotaA>250.00</KwotaA><KwotaB>200.00</KwotaB></Pasywa_A>
    </Pasywa>
  </BilansJednostkaMala>
  <RZiSJednostkaMala>
    <RZiSPor>
      <A><KwotaA>800.00</KwotaA><KwotaB>700.00</KwotaB></A>
      <H><KwotaA>50.00</KwotaA><KwotaB>40.00</KwotaB></H>
      <J><KwotaA>35.00</KwotaA><KwotaB>30.00</KwotaB></J>
    </RZiSPor>
  </RZiSJednostkaMala>
</JednostkaMala>
"""

SFJMIZ_XML = """<?xml version="1.0"?>
<JednostkaMikro>
  <InformacjeOgolneJednostkaMikro>
    <P_1><NazwaFirmy>Mikro Sp. z o.o.</NazwaFirmy></P_1>
    <P_1D>1111111111</P_1D>
    <P_1E>0000099999</P_1E>
    <P_3><DataOd>2023-01-01</DataOd><DataDo>2023-12-31</DataDo></P_3>
    <DataSporzadzenia>2024-05-01</DataSporzadzenia>
  </InformacjeOgolneJednostkaMikro>
  <BilansJednostkaMikro>
    <Aktywa><KwotaA>100.00</KwotaA><KwotaB>80.00</KwotaB>
      <Aktywa_A><KwotaA>30.00</KwotaA><KwotaB>25.00</KwotaB></Aktywa_A>
      <Aktywa_B><KwotaA>70.00</KwotaA><KwotaB>55.00</KwotaB></Aktywa_B>
    </Aktywa>
    <Pasywa><KwotaA>100.00</KwotaA><KwotaB>80.00</KwotaB>
      <Pasywa_A><KwotaA>60.00</KwotaA><KwotaB>50.00</KwotaB></Pasywa_A>
      <Pasywa_B><KwotaA>40.00</KwotaA><KwotaB>30.00</KwotaB></Pasywa_B>
    </Pasywa>
  </BilansJednostkaMikro>
  <RZiSJednostkaMikro>
    <A><KwotaA>200.00</KwotaA><KwotaB>180.00</KwotaB></A>
    <B><KwotaA>150.00</KwotaA><KwotaB>140.00</KwotaB></B>
    <F><KwotaA>20.00</KwotaA><KwotaB>15.00</KwotaB></F>
  </RZiSJednostkaMikro>
</JednostkaMikro>
"""

SFJOPZ_XML = """<?xml version="1.0"?>
<JednostkaOp>
  <WprowadzenieDoSprawozdaniaFinansowegoJednostkaOp>
    <P_1><NazwaFirmy>Fundacja Test</NazwaFirmy></P_1>
    <P_1D>2222222222</P_1D>
    <P_1E>0000011111</P_1E>
    <P_3><DataOd>2023-01-01</DataOd><DataDo>2023-12-31</DataDo></P_3>
    <DataSporzadzenia>2024-06-01</DataSporzadzenia>
  </WprowadzenieDoSprawozdaniaFinansowegoJednostkaOp>
  <BilansJednostkaOp>
    <Aktywa><KwotaA>300.00</KwotaA><KwotaB>250.00</KwotaB>
      <Aktywa_A><KwotaA>150.00</KwotaA><KwotaB>130.00</KwotaB></Aktywa_A>
      <Aktywa_B><KwotaA>150.00</KwotaA><KwotaB>120.00</KwotaB></Aktywa_B>
    </Aktywa>
    <Pasywa><KwotaA>300.00</KwotaA><KwotaB>250.00</KwotaB>
      <Pasywa_A><KwotaA>200.00</KwotaA><KwotaB>180.00</KwotaB></Pasywa_A>
    </Pasywa>
  </BilansJednostkaOp>
  <RZiSJednostkaOp>
    <A><KwotaA>500.00</KwotaA><KwotaB>400.00</KwotaB>
      <A_I><KwotaA>300.00</KwotaA><KwotaB>250.00</KwotaB></A_I>
      <A_II><KwotaA>200.00</KwotaA><KwotaB>150.00</KwotaB></A_II>
    </A>
    <O><KwotaA>25.00</KwotaA><KwotaB>20.00</KwotaB></O>
  </RZiSJednostkaOp>
</JednostkaOp>
"""

SFZURT_XML = """<?xml version="1.0"?>
<ZakladUbezpieczenWTys>
  <Wstep>
    <P_1><NazwaFirmy>Ubezpieczalnia TU SA</NazwaFirmy></P_1>
    <P_1B>3333333333</P_1B>
    <P_1C>0000033333</P_1C>
    <P_2A><DataOd>2023-01-01</DataOd><DataDo>2023-12-31</DataDo></P_2A>
    <DataSporzadzenia>2024-04-30</DataSporzadzenia>
  </Wstep>
  <BilansZakladUbezpieczen>
    <Aktywa><KwotaA>5000.00</KwotaA><KwotaB>4500.00</KwotaB><KwotaB1>4400.00</KwotaB1>
      <Aktywa_D><KwotaA>3000.00</KwotaA><KwotaB>2800.00</KwotaB><KwotaB1>2700.00</KwotaB1></Aktywa_D>
    </Aktywa>
    <Pasywa><KwotaA>5000.00</KwotaA><KwotaB>4500.00</KwotaB><KwotaB1>4400.00</KwotaB1>
      <Pasywa_A><KwotaA>1000.00</KwotaA><KwotaB>900.00</KwotaB><KwotaB1>880.00</KwotaB1></Pasywa_A>
    </Pasywa>
  </BilansZakladUbezpieczen>
  <RZiSZakladUbezpieczen>
    <I><KwotaA>2000.00</KwotaA><KwotaB>1800.00</KwotaB></I>
    <XIV><KwotaA>150.00</KwotaA><KwotaB>120.00</KwotaB></XIV>
  </RZiSZakladUbezpieczen>
  <PrzeplywyZakladUbezpieczen>
    <A_I><KwotaA>800.00</KwotaA><KwotaB>700.00</KwotaB></A_I>
    <D><KwotaA>50.00</KwotaA><KwotaB>30.00</KwotaB></D>
  </PrzeplywyZakladUbezpieczen>
</ZakladUbezpieczenWTys>
"""


# ---------------------------------------------------------------------------
# Schema detection + parsing tests
# ---------------------------------------------------------------------------

class TestSFJINZParsing:
    def test_schema_code(self):
        result = xml_parser.parse_statement(SFJINZ_XML)
        assert result["company"]["schema_code"] == "SFJINZ"

    def test_bilans(self):
        result = xml_parser.parse_statement(SFJINZ_XML)
        assert result["bilans"]["aktywa"] is not None
        assert result["bilans"]["aktywa"]["kwota_a"] == 1000.0

    def test_rzis_porownawczy(self):
        result = xml_parser.parse_statement(SFJINZ_XML)
        assert result["company"]["rzis_variant"] == "porownawczy"
        tags = [n["tag"] for n in result["rzis"]]
        assert "RZiS.A" in tags
        assert "RZiS.L" in tags

    def test_cash_flow(self):
        result = xml_parser.parse_statement(SFJINZ_XML)
        assert result["company"]["cf_method"] == "posrednia"
        assert len(result["cash_flow"]) > 0

    def test_rzis_kalkulacyjny_variant(self):
        xml = SFJINZ_XML.replace("<RZiSPor>", "<RZiSKalk>").replace("</RZiSPor>", "</RZiSKalk>")
        result = xml_parser.parse_statement(xml)
        assert result["company"]["rzis_variant"] == "kalkulacyjny"
        tags = [n["tag"] for n in result["rzis"]]
        assert "RZiS.A" in tags
        assert "RZiS.L" in tags

    def test_labels(self):
        result = xml_parser.parse_statement(SFJINZ_XML)
        assert result["bilans"]["aktywa"]["label"] == "AKTYWA"


class TestSFJMAZParsing:
    def test_schema_code(self):
        result = xml_parser.parse_statement(SFJMAZ_XML)
        assert result["company"]["schema_code"] == "SFJMAZ"

    def test_bilans(self):
        result = xml_parser.parse_statement(SFJMAZ_XML)
        assert result["bilans"]["aktywa"]["kwota_a"] == 500.0

    def test_rzis_with_variants(self):
        result = xml_parser.parse_statement(SFJMAZ_XML)
        assert result["company"]["rzis_variant"] == "porownawczy"
        tags = [n["tag"] for n in result["rzis"]]
        assert "RZiS.A" in tags
        assert "RZiS.H" in tags

    def test_no_cash_flow(self):
        result = xml_parser.parse_statement(SFJMAZ_XML)
        assert result["cash_flow"] == []

    def test_rzis_kalkulacyjny_variant(self):
        xml = SFJMAZ_XML.replace("<RZiSPor>", "<RZiSKalk>").replace("</RZiSPor>", "</RZiSKalk>")
        result = xml_parser.parse_statement(xml)
        assert result["company"]["rzis_variant"] == "kalkulacyjny"
        tags = [n["tag"] for n in result["rzis"]]
        assert "RZiS.A" in tags
        assert "RZiS.H" in tags


class TestSFJMIZParsing:
    def test_schema_code(self):
        result = xml_parser.parse_statement(SFJMIZ_XML)
        assert result["company"]["schema_code"] == "SFJMIZ"

    def test_bilans(self):
        result = xml_parser.parse_statement(SFJMIZ_XML)
        assert result["bilans"]["aktywa"]["kwota_a"] == 100.0
        assert result["bilans"]["pasywa"]["kwota_a"] == 100.0

    def test_rzis_no_variant_wrapper(self):
        """SFJMIZ RZiS has no RZiSPor/RZiSKalk wrapper — direct children."""
        result = xml_parser.parse_statement(SFJMIZ_XML)
        assert result["company"]["rzis_variant"] is None
        tags = [n["tag"] for n in result["rzis"]]
        assert "RZiS.A" in tags
        assert "RZiS.F" in tags

    def test_no_cash_flow(self):
        result = xml_parser.parse_statement(SFJMIZ_XML)
        assert result["cash_flow"] == []

    def test_labels(self):
        result = xml_parser.parse_statement(SFJMIZ_XML)
        assert "mikro" in result["bilans"]["aktywa"]["children"][0]["label"].lower() or \
               result["bilans"]["aktywa"]["children"][0]["label"].startswith("A.")


class TestSFJOPZParsing:
    def test_schema_code(self):
        result = xml_parser.parse_statement(SFJOPZ_XML)
        assert result["company"]["schema_code"] == "SFJOPZ"

    def test_bilans(self):
        result = xml_parser.parse_statement(SFJOPZ_XML)
        assert result["bilans"]["aktywa"]["kwota_a"] == 300.0

    def test_rzis_opp_structure(self):
        """OPP RZiS has A through O with statutory activity categories."""
        result = xml_parser.parse_statement(SFJOPZ_XML)
        assert result["company"]["rzis_variant"] is None
        tags = [n["tag"] for n in result["rzis"]]
        assert "RZiS.A" in tags
        assert "RZiS.O" in tags

    def test_labels_use_fundusz(self):
        """OPP uses 'Fundusz wlasny' instead of 'Kapital wlasny'."""
        result = xml_parser.parse_statement(SFJOPZ_XML)
        pasywa_a = result["bilans"]["pasywa"]["children"][0]
        assert "Fundusz" in pasywa_a["label"]


class TestSFZURTParsing:
    def test_schema_code(self):
        result = xml_parser.parse_statement(SFZURT_XML)
        assert result["company"]["schema_code"] == "SFZURT"

    def test_unit_multiplier(self):
        """SFZURT amounts in XML are thousands of PLN; parser multiplies by 1000."""
        result = xml_parser.parse_statement(SFZURT_XML)
        assert result["bilans"]["aktywa"]["kwota_a"] == 5_000_000.0  # 5000 * 1000

    def test_krs_from_p1c(self):
        """Insurance XML uses P_1C for KRS (not P_1E)."""
        result = xml_parser.parse_statement(SFZURT_XML)
        assert result["company"]["krs"] == "0000033333"

    def test_nip_from_p1b(self):
        result = xml_parser.parse_statement(SFZURT_XML)
        assert result["company"]["nip"] == "3333333333"

    def test_rzis_roman_numerals(self):
        """Insurance RZiS uses Roman numeral items (I, XIV, etc.)."""
        result = xml_parser.parse_statement(SFZURT_XML)
        tags = [n["tag"] for n in result["rzis"]]
        assert "RZiS.I" in tags
        assert "RZiS.XIV" in tags

    def test_cash_flow_present(self):
        result = xml_parser.parse_statement(SFZURT_XML)
        assert len(result["cash_flow"]) > 0

    def test_kwota_b1(self):
        """Insurance bilans has three amount columns including KwotaB1."""
        result = xml_parser.parse_statement(SFZURT_XML)
        assert result["bilans"]["aktywa"]["kwota_b1"] == 4_400_000.0  # 4400 * 1000

    def test_period_from_p2a(self):
        """Insurance uses P_2A for period dates."""
        result = xml_parser.parse_statement(SFZURT_XML)
        assert result["company"]["period_start"] == "2023-01-01"
        assert result["company"]["period_end"] == "2023-12-31"

    def test_wzlotych_root_uses_pln_multiplier(self):
        xml = SFZURT_XML.replace(
            "<ZakladUbezpieczenWTys>",
            "<SprFinZakladUbezpieczenWZlotych>",
        ).replace(
            "</ZakladUbezpieczenWTys>",
            "</SprFinZakladUbezpieczenWZlotych>",
        )
        result = xml_parser.parse_statement(xml)
        assert result["company"]["schema_code"] == "SFZURT"
        assert result["bilans"]["aktywa"]["kwota_a"] == 5000.0


class TestDispatchByKodSystemowy:
    def test_unknown_root_dispatches_from_kod_systemowy(self):
        xml = """<?xml version="1.0"?>
<UnknownEnvelope>
  <Naglowek><KodSprawozdania kodSystemowy="SFJMIZ (1)" wersjaSchemy="1-2"/></Naglowek>
  <BilansJednostkaMikro>
    <Aktywa><KwotaA>2.00</KwotaA><KwotaB>1.00</KwotaB></Aktywa>
    <Pasywa><KwotaA>2.00</KwotaA><KwotaB>1.00</KwotaB></Pasywa>
  </BilansJednostkaMikro>
  <RZiSJednostkaMikro>
    <A><KwotaA>3.00</KwotaA><KwotaB>2.00</KwotaB></A>
  </RZiSJednostkaMikro>
</UnknownEnvelope>
"""
        result = xml_parser.parse_statement(xml)
        assert result["company"]["schema_code"] == "SFJMIZ"
        assert result["bilans"]["aktywa"]["kwota_a"] == 2.0
        assert result["rzis"][0]["tag"] == "RZiS.A"

    def test_kod_systemowy_thousands_variant_applies_multiplier(self):
        xml = """<?xml version="1.0"?>
<UnknownEnvelope>
  <Naglowek><KodSprawozdania kodSystemowy="SFJINT (2)" wersjaSchemy="1-3"/></Naglowek>
  <Bilans>
    <Aktywa><KwotaA>2.00</KwotaA><KwotaB>1.00</KwotaB></Aktywa>
    <Pasywa><KwotaA>2.00</KwotaA><KwotaB>1.00</KwotaB></Pasywa>
  </Bilans>
  <RZiS>
    <RZiSPor>
      <A><KwotaA>3.00</KwotaA><KwotaB>2.00</KwotaB></A>
    </RZiSPor>
  </RZiS>
</UnknownEnvelope>
"""
        result = xml_parser.parse_statement(xml)
        assert result["company"]["schema_code"] == "SFJINZ"
        assert result["bilans"]["aktywa"]["kwota_a"] == 2000.0
        assert result["rzis"][0]["kwota_a"] == 3000.0


class TestThousandsCurrencyVariants:
    @pytest.mark.parametrize(
        "xml_text,expected_schema",
        [
            (
                """<?xml version="1.0"?>
<SprFinJednostkaInnaWTysiacach>
  <Bilans>
    <Aktywa><KwotaA>1.00</KwotaA><KwotaB>1.00</KwotaB></Aktywa>
    <Pasywa><KwotaA>1.00</KwotaA><KwotaB>1.00</KwotaB></Pasywa>
  </Bilans>
  <RZiS><RZiSPor><A><KwotaA>1.00</KwotaA><KwotaB>1.00</KwotaB></A></RZiSPor></RZiS>
</SprFinJednostkaInnaWTysiacach>
""",
                "SFJINZ",
            ),
            (
                """<?xml version="1.0"?>
<SprFinJednostkaMalaWTysiacach>
  <BilansJednostkaMala>
    <Aktywa><KwotaA>1.00</KwotaA><KwotaB>1.00</KwotaB></Aktywa>
    <Pasywa><KwotaA>1.00</KwotaA><KwotaB>1.00</KwotaB></Pasywa>
  </BilansJednostkaMala>
  <RZiSJednostkaMala><RZiSPor><A><KwotaA>1.00</KwotaA><KwotaB>1.00</KwotaB></A></RZiSPor></RZiSJednostkaMala>
</SprFinJednostkaMalaWTysiacach>
""",
                "SFJMAZ",
            ),
            (
                """<?xml version="1.0"?>
<SprFinJednostkaMikroWTysiacach>
  <BilansJednostkaMikro>
    <Aktywa><KwotaA>1.00</KwotaA><KwotaB>1.00</KwotaB></Aktywa>
    <Pasywa><KwotaA>1.00</KwotaA><KwotaB>1.00</KwotaB></Pasywa>
  </BilansJednostkaMikro>
  <RZiSJednostkaMikro><A><KwotaA>1.00</KwotaA><KwotaB>1.00</KwotaB></A></RZiSJednostkaMikro>
</SprFinJednostkaMikroWTysiacach>
""",
                "SFJMIZ",
            ),
            (
                """<?xml version="1.0"?>
<SprFinJednostkaOPPWTysiacach>
  <BilansJednostkaOp>
    <Aktywa><KwotaA>1.00</KwotaA><KwotaB>1.00</KwotaB></Aktywa>
    <Pasywa><KwotaA>1.00</KwotaA><KwotaB>1.00</KwotaB></Pasywa>
  </BilansJednostkaOp>
  <RZiSJednostkaOp><A><KwotaA>1.00</KwotaA><KwotaB>1.00</KwotaB></A></RZiSJednostkaOp>
</SprFinJednostkaOPPWTysiacach>
""",
                "SFJOPZ",
            ),
        ],
    )
    def test_thousands_roots_scale_amounts(self, xml_text, expected_schema):
        result = xml_parser.parse_statement(xml_text)
        assert result["company"]["schema_code"] == expected_schema
        assert result["bilans"]["aktywa"]["kwota_a"] == 1000.0


# ---------------------------------------------------------------------------
# Marker detection tests
# ---------------------------------------------------------------------------

class TestStatementMarkerDetection:
    def test_standard_bilans(self):
        assert xml_parser._is_statement_marker("Bilans")

    def test_schema_suffixed_bilans(self):
        assert xml_parser._is_statement_marker("BilansJednostkaMala")
        assert xml_parser._is_statement_marker("BilansJednostkaMikro")
        assert xml_parser._is_statement_marker("BilansJednostkaOp")
        assert xml_parser._is_statement_marker("BilansZakladUbezpieczen")

    def test_rzis_variants(self):
        assert xml_parser._is_statement_marker("RZiS")
        assert xml_parser._is_statement_marker("RZiSJednostkaMala")
        assert xml_parser._is_statement_marker("RZiSZakladUbezpieczen")

    def test_cash_flow_variants(self):
        assert xml_parser._is_statement_marker("RachPrzeplywow")
        assert xml_parser._is_statement_marker("PrzeplywyZakladUbezpieczen")

    def test_non_markers(self):
        assert not xml_parser._is_statement_marker("NazwaFirmy")
        assert not xml_parser._is_statement_marker("KwotaA")


# ---------------------------------------------------------------------------
# Backward compatibility
# ---------------------------------------------------------------------------

class TestBackwardCompatibility:
    def test_tag_labels_alias(self):
        """TAG_LABELS should still be importable and contain SFJINZ labels."""
        assert xml_parser.TAG_LABELS["Aktywa"] == "AKTYWA"
        assert "RZiS.L" not in xml_parser.TAG_LABELS  # RZiS uses raw keys
        assert xml_parser.TAG_LABELS["L"] == "L. Zysk (strata) netto (I-J-K)"

    def test_unknown_schema_falls_back_to_sfjinz(self):
        """Unknown root tag should fall back to SFJINZ parsing."""
        xml = SFJINZ_XML.replace("<JednostkaInna>", "<UnknownRoot>").replace(
            "</JednostkaInna>", "</UnknownRoot>"
        )
        result = xml_parser.parse_statement(xml)
        assert result["company"]["schema_code"] == "SFJINZ"
        assert result["bilans"]["aktywa"] is not None


# ---------------------------------------------------------------------------
# Semantic tag resolution (RA-001 / RA-002)
# ---------------------------------------------------------------------------

class TestSemanticTagResolution:
    """Tests for resolve_tag() — maps concepts to per-schema tag_paths."""

    @pytest.mark.parametrize("schema_code,expected_tag", [
        ("SFJINZ", "RZiS.A"),
        ("SFJMAZ", "RZiS.A"),
        ("SFJMIZ", "RZiS.A"),
        ("SFJOPZ", "RZiS.A"),
        ("SFZURT", "RZiS.I"),
    ])
    def test_revenue_resolves_for_all_schemas(self, schema_code, expected_tag):
        assert xml_parser.resolve_tag("revenue", schema_code) == expected_tag

    @pytest.mark.parametrize("schema_code,expected_tag", [
        ("SFJINZ", "RZiS.L"),
        ("SFJMAZ", "RZiS.J"),
        ("SFJMIZ", "RZiS.F"),
        ("SFJOPZ", "RZiS.O"),
    ])
    def test_net_profit_resolves_for_standard_schemas(self, schema_code, expected_tag):
        assert xml_parser.resolve_tag("net_profit", schema_code) == expected_tag

    def test_sfzurt_net_profit_is_none(self):
        """SFZURT has no single net-profit line — XIV is gross profit (before tax)."""
        assert xml_parser.resolve_tag("net_profit", "SFZURT") is None

    def test_sfzurt_gross_profit_maps_to_xiv(self):
        assert xml_parser.resolve_tag("gross_profit", "SFZURT") == "RZiS.XIV"

    def test_unknown_concept_returns_none(self):
        assert xml_parser.resolve_tag("nonexistent_concept", "SFJINZ") is None

    def test_missing_schema_returns_none(self):
        assert xml_parser.resolve_tag("revenue", "UNKNOWN_SCHEMA") is None


class TestSchemaAwareRatios:
    """compute_ratios must produce correct results for each schema type."""

    def test_sfjinz_ratios(self):
        result = xml_parser.parse_statement(SFJINZ_XML)
        ratios = xml_parser.compute_ratios(result)
        assert ratios["equity_ratio"] is not None
        assert ratios["net_margin"] is not None

    def test_sfjmaz_ratios(self):
        result = xml_parser.parse_statement(SFJMAZ_XML)
        ratios = xml_parser.compute_ratios(result)
        assert ratios["equity_ratio"] is not None
        # SFJMAZ net profit is at RZiS.J
        assert ratios["net_margin"] is not None

    def test_sfjmiz_ratios(self):
        result = xml_parser.parse_statement(SFJMIZ_XML)
        ratios = xml_parser.compute_ratios(result)
        assert ratios["equity_ratio"] is not None
        # SFJMIZ net profit at RZiS.F
        assert ratios["net_margin"] is not None

    def test_sfjopz_ratios(self):
        result = xml_parser.parse_statement(SFJOPZ_XML)
        ratios = xml_parser.compute_ratios(result)
        assert ratios["equity_ratio"] is not None
        # SFJOPZ net profit at RZiS.O
        assert ratios["net_margin"] is not None

    def test_sfzurt_net_margin_is_none(self):
        """SFZURT has no net-profit line, so net_margin must be None."""
        result = xml_parser.parse_statement(SFZURT_XML)
        ratios = xml_parser.compute_ratios(result)
        assert ratios["net_margin"] is None

    def test_sfzurt_equity_ratio_uses_multiplier(self):
        """Insurance amounts are in thousands — verify ratios use scaled values."""
        result = xml_parser.parse_statement(SFZURT_XML)
        ratios = xml_parser.compute_ratios(result)
        # equity (1000*1000) / assets (5000*1000) = 0.2
        assert ratios["equity_ratio"] == 0.2


# ---------------------------------------------------------------------------
# Extra sections parsing (RA-003)
# ---------------------------------------------------------------------------

SFJINZ_WITH_EQUITY_CHANGES_XML = """<?xml version="1.0"?>
<JednostkaInna>
  <WprowadzenieDoSprawozdaniaFinansowego>
    <P_1><NazwaFirmy>Test SA</NazwaFirmy></P_1>
    <P_1E>0000012345</P_1E>
    <P_3><DataOd>2023-01-01</DataOd><DataDo>2023-12-31</DataDo></P_3>
    <DataSporzadzenia>2024-03-15</DataSporzadzenia>
  </WprowadzenieDoSprawozdaniaFinansowego>
  <Bilans>
    <Aktywa><KwotaA>1000.00</KwotaA><KwotaB>900.00</KwotaB></Aktywa>
    <Pasywa><KwotaA>1000.00</KwotaA><KwotaB>900.00</KwotaB></Pasywa>
  </Bilans>
  <RZiS><RZiSPor><A><KwotaA>500.00</KwotaA><KwotaB>400.00</KwotaB></A></RZiSPor></RZiS>
  <ZestZmianWKapitale>
    <A_I><KwotaA>100.00</KwotaA><KwotaB>90.00</KwotaB></A_I>
    <A_II><KwotaA>50.00</KwotaA><KwotaB>45.00</KwotaB></A_II>
  </ZestZmianWKapitale>
</JednostkaInna>
"""

SFZURT_WITH_OBS_XML = """<?xml version="1.0"?>
<ZakladUbezpieczenWTys>
  <Wstep>
    <P_1><NazwaFirmy>UB TU SA</NazwaFirmy></P_1>
    <P_1C>0000033333</P_1C>
    <P_2A><DataOd>2023-01-01</DataOd><DataDo>2023-12-31</DataDo></P_2A>
    <DataSporzadzenia>2024-04-30</DataSporzadzenia>
  </Wstep>
  <BilansZakladUbezpieczen>
    <Aktywa><KwotaA>5000.00</KwotaA><KwotaB>4500.00</KwotaB></Aktywa>
    <Pasywa><KwotaA>5000.00</KwotaA><KwotaB>4500.00</KwotaB></Pasywa>
  </BilansZakladUbezpieczen>
  <RZiSZakladUbezpieczen>
    <I><KwotaA>2000.00</KwotaA><KwotaB>1800.00</KwotaB></I>
  </RZiSZakladUbezpieczen>
  <PozabilansoweZakladUbezpieczen>
    <P_1><KwotaA>300.00</KwotaA><KwotaB>250.00</KwotaB></P_1>
    <P_2><KwotaA>100.00</KwotaA><KwotaB>80.00</KwotaB></P_2>
  </PozabilansoweZakladUbezpieczen>
</ZakladUbezpieczenWTys>
"""


class TestExtrasSectionParsing:
    """Tests for extra sections: ZestZmianWKapitale, Pozabilansowe."""

    def test_sfjinz_equity_changes_parsed(self):
        result = xml_parser.parse_statement(SFJINZ_WITH_EQUITY_CHANGES_XML)
        assert "extras" in result
        assert "equity_changes" in result["extras"]
        nodes = result["extras"]["equity_changes"]
        assert len(nodes) == 2
        tags = [n["tag"] for n in nodes]
        assert "EQ.A_I" in tags
        assert "EQ.A_II" in tags

    def test_sfjinz_equity_changes_values(self):
        result = xml_parser.parse_statement(SFJINZ_WITH_EQUITY_CHANGES_XML)
        nodes = result["extras"]["equity_changes"]
        a_i = next(n for n in nodes if n["tag"] == "EQ.A_I")
        assert a_i["kwota_a"] == 100.0
        assert a_i["kwota_b"] == 90.0

    def test_sfzurt_off_balance_sheet_parsed(self):
        result = xml_parser.parse_statement(SFZURT_WITH_OBS_XML)
        assert "off_balance_sheet" in result["extras"]
        nodes = result["extras"]["off_balance_sheet"]
        assert len(nodes) == 2
        tags = [n["tag"] for n in nodes]
        assert "OBS.P_1" in tags
        assert "OBS.P_2" in tags

    def test_sfzurt_off_balance_sheet_multiplied(self):
        """Off-balance-sheet amounts should also be multiplied by 1000."""
        result = xml_parser.parse_statement(SFZURT_WITH_OBS_XML)
        p1 = next(n for n in result["extras"]["off_balance_sheet"] if n["tag"] == "OBS.P_1")
        assert p1["kwota_a"] == 300_000.0  # 300 * 1000

    def test_no_extras_when_section_absent(self):
        result = xml_parser.parse_statement(SFJINZ_XML)
        assert result["extras"] == {}

    def test_sfjmiz_has_no_extra_sections(self):
        result = xml_parser.parse_statement(SFJMIZ_XML)
        assert result["extras"] == {}


# ---------------------------------------------------------------------------
# Cross-schema compare deltas (FS-001)
# ---------------------------------------------------------------------------

class TestCrossSchemaCompare:
    """Verify revenue/net-profit YoY deltas work when schemas differ between periods."""

    def test_cross_schema_revenue_delta(self):
        """SFZURT revenue=RZiS.I, SFJINZ revenue=RZiS.A — delta must use correct tags."""
        current = xml_parser.parse_statement(SFZURT_XML)   # revenue at RZiS.I = 2000 (×1000)
        previous = xml_parser.parse_statement(SFJINZ_XML)  # revenue at RZiS.A = 2000

        cur_schema = current["company"]["schema_code"]
        prev_schema = previous["company"]["schema_code"]

        tag_curr = xml_parser.resolve_tag("revenue", cur_schema)
        tag_prev = xml_parser.resolve_tag("revenue", prev_schema)
        assert tag_curr == "RZiS.I"
        assert tag_prev == "RZiS.A"

        a = xml_parser.find_value(current, tag_curr)
        b = xml_parser.find_value(previous, tag_prev)
        assert a is not None and b is not None
        # Both values exist, so the delta is computable
        assert round((a - b) / abs(b) * 100, 2) is not None

    def test_cross_schema_net_profit_sfzurt_is_none(self):
        """SFZURT has no net_profit mapping, so delta must be None."""
        current = xml_parser.parse_statement(SFZURT_XML)
        cur_schema = current["company"]["schema_code"]
        assert xml_parser.resolve_tag("net_profit", cur_schema) is None

    def test_cross_schema_net_profit_sfjmiz_to_sfjinz(self):
        """SFJMIZ net_profit=RZiS.F, SFJINZ=RZiS.L — different tags, both valid."""
        current = xml_parser.parse_statement(SFJMIZ_XML)
        previous = xml_parser.parse_statement(SFJINZ_XML)

        tag_curr = xml_parser.resolve_tag("net_profit", current["company"]["schema_code"])
        tag_prev = xml_parser.resolve_tag("net_profit", previous["company"]["schema_code"])
        assert tag_curr == "RZiS.F"
        assert tag_prev == "RZiS.L"

        a = xml_parser.find_value(current, tag_curr)
        b = xml_parser.find_value(previous, tag_prev)
        assert a is not None  # 20.0
        assert b is not None  # 100.0


# ---------------------------------------------------------------------------
# Extras in flat extraction and find_value (FS-003)
# ---------------------------------------------------------------------------

class TestExtrasInFlatExtraction:
    """extract_flat_values and find_value must include extras sections."""

    def test_flat_values_includes_extras(self):
        result = xml_parser.parse_statement(SFJINZ_WITH_EQUITY_CHANGES_XML)
        flat = xml_parser.extract_flat_values(result)
        assert "EQ.A_I" in flat
        assert flat["EQ.A_I"] == 100.0

    def test_flat_values_kwota_b_includes_extras(self):
        result = xml_parser.parse_statement(SFJINZ_WITH_EQUITY_CHANGES_XML)
        flat = xml_parser.extract_flat_values(result, use_kwota_b=True)
        assert flat["EQ.A_I"] == 90.0

    def test_find_value_searches_extras(self):
        result = xml_parser.parse_statement(SFJINZ_WITH_EQUITY_CHANGES_XML)
        v = xml_parser.find_value(result, "EQ.A_II")
        assert v == 50.0

    def test_flat_values_no_extras_returns_standard_only(self):
        result = xml_parser.parse_statement(SFJINZ_XML)
        flat = xml_parser.extract_flat_values(result)
        assert "EQ.A_I" not in flat
        assert "Aktywa" in flat
