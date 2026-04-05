from unittest.mock import patch

from app.services import predictions


def test_build_features_includes_formula_tags_missing_from_required_tags():
    feature_rows = [
        {
            "feature_definition_id": "x1_maczynska",
            "name": "X1",
            "category": "profitability",
            "value": 0.33,
            "formula_description": "(RZiS.I + CF.A_II_1) / Pasywa_B",
            # Simulates stale DB metadata missing the CF tag.
            "required_tags": ["RZiS.I", "Pasywa_B"],
            "computation_logic": "custom",
        }
    ]
    source_rows = [
        {
            "tag_path": "RZiS.I",
            "label_pl": "Zysk (strata) brutto",
            "value_current": 120.0,
            "value_previous": 90.0,
            "section": "RZiS",
        },
        {
            "tag_path": "Pasywa_B",
            "label_pl": "Zobowiazania i rezerwy na zobowiazania",
            "value_current": 400.0,
            "value_previous": 350.0,
            "section": "Bilans",
        },
    ]

    with (
        patch(
            "app.db.prediction_db.get_features_for_prediction",
            return_value=feature_rows,
        ) as mock_features,
        patch(
            "app.db.prediction_db.get_source_line_items_for_report",
            return_value=source_rows,
        ) as mock_sources,
    ):
        result = predictions._build_features(
            report_id="rpt-1",
            feature_set_id="maczynska_6",
            contributions={"x1_maczynska": 0.5},
            scored_at="2026-03-01 12:00:00",
            schema_code="SFJINZ",
        )

    mock_features.assert_called_once()
    requested_tags = mock_sources.call_args.args[1]
    assert "CF.A_II_1" in requested_tags

    assert len(result) == 1
    source_tags = result[0]["source_tags"]
    assert [t["tag_path"] for t in source_tags] == ["RZiS.I", "CF.A_II_1", "Pasywa_B"]

    cf_tag = source_tags[1]
    assert cf_tag["value_current"] is None
    assert cf_tag["value_previous"] is None
    assert cf_tag["label_pl"] == "1. Amortyzacja"
    assert cf_tag["section"] == "CF"


# ---------------------------------------------------------------------------
# BE-PRED-010 / CR4-002: semantic direction registry lookup
# ---------------------------------------------------------------------------


class TestResolveHigherIsBetter:
    """Exact-match lookups against `_TAG_SEMANTIC_REGISTRY`.

    The registry is intentionally exact per tag_path (no prefix inheritance).
    These tests lock in current behavior so future edits cannot silently
    change semantics for sibling tags or previously-neutral tags.
    """

    def test_registered_true_tags(self):
        for tag in (
            "RZiS.A",
            "RZiS.C",
            "RZiS.F",
            "RZiS.I",
            "RZiS.L",
            "CF.A_II_1",
            "Pasywa_A",
        ):
            assert predictions._resolve_higher_is_better(tag) is True, tag

    def test_registered_false_tags(self):
        for tag in ("Pasywa_B", "Aktywa_B_I"):
            assert predictions._resolve_higher_is_better(tag) is False, tag

    def test_unknown_tags_return_none(self):
        """Anything not in the registry must resolve to None — no guessing."""
        for tag in (
            "Aktywa",            # neutral: size proxy
            "Aktywa_A",          # neutral
            "Aktywa_B",          # neutral (broader bucket)
            "Aktywa_B_II",       # context-dependent (receivables)
            "Aktywa_B_III",      # context-dependent (cash)
            "Pasywa_B_III",      # child of Pasywa_B but NOT auto-inherited
            "RZiS.B",            # operating costs — not in registry
            "CF.D",              # net cash flow — not in registry
            "SomethingUnknown",  # totally unknown
        ):
            assert predictions._resolve_higher_is_better(tag) is None, tag

    def test_sibling_tags_are_not_inherited_from_registered_parents(self):
        """`Pasywa_B` is False but `Pasywa_B_III` must NOT auto-inherit.

        Regression guard: the previous prefix-based implementation would
        falsely tag every `Pasywa_B_*` child as False. Explicit-registry
        behavior requires an intentional per-tag entry instead.
        """
        assert predictions._resolve_higher_is_better("Pasywa_B") is False
        assert predictions._resolve_higher_is_better("Pasywa_B_III") is None

    def test_cf_a_ii_1_exact_match_not_prefix(self):
        """`CF.A_II_1` is registered; neighbors like `CF.A_II_2` must be None."""
        assert predictions._resolve_higher_is_better("CF.A_II_1") is True
        assert predictions._resolve_higher_is_better("CF.A_II_2") is None
        assert predictions._resolve_higher_is_better("CF.A") is None

    def test_empty_and_unusual_paths(self):
        """Defensive: empty/weird tag paths resolve to None without errors."""
        assert predictions._resolve_higher_is_better("") is None
        assert predictions._resolve_higher_is_better("rzis.i") is None  # case-sensitive
        assert predictions._resolve_higher_is_better("RZiS") is None  # bare section
