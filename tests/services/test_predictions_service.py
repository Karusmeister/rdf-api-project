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
