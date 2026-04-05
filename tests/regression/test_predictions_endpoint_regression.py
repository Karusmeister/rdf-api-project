"""Regression tests for authenticated predictions endpoints.

Guards that the app lifespan — not a scoring side effect — is what makes
built-in models discoverable through `/api/predictions/models`. Lives in the
regression layer because it runs the real FastAPI lifespan through
`live_app_client` (same fixture as the RDF/KRS regression suites) instead of
TestClient shortcuts.

CR-PZN-001 / CR-PZN-005.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.regression]


@pytest.mark.asyncio
async def test_models_catalog_includes_builtin_models_after_startup(live_app_client):
    """Every built-in model must be visible immediately after lifespan boot.

    Before CR-PZN-001 a freshly deployed environment had to run `score_batch`
    at least once AND flush caches before `/api/predictions/models` would
    include new models. Now `register_builtin_models()` runs in the lifespan,
    so the catalog endpoint is authoritative from the very first request and
    no manual admin step is required on deploy.
    """
    response = await live_app_client.get("/api/predictions/models")
    assert response.status_code == 200, response.text

    payload = response.json()
    assert "models" in payload
    ids = {m["model_id"] for m in payload["models"]}

    assert "maczynska_1994_v1" in ids, (
        "Maczynska model missing from /api/predictions/models — "
        "startup registration regression."
    )
    assert "poznanski_2004_v1" in ids, (
        "Poznanski model missing from /api/predictions/models — "
        "CR-PZN-001 regression: new built-in models must register in lifespan."
    )

    poznanski = next(
        m for m in payload["models"] if m["model_id"] == "poznanski_2004_v1"
    )
    # Interpretation guide is part of the contract clients render without
    # calling the per-KRS endpoint, so verify it survives the catalog round-trip.
    interpretation = poznanski["interpretation"]
    assert interpretation is not None, "Poznanski model must expose interpretation"
    assert interpretation["higher_is_better"] is True
    labels = {t["label"] for t in interpretation["thresholds"]}
    assert {"critical", "medium", "low"}.issubset(labels), (
        f"Poznanski thresholds missing expected bands; got {labels}"
    )


@pytest.mark.asyncio
async def test_predictions_endpoint_requires_authentication(live_app_client):
    """Unauthenticated calls to `/api/predictions/{krs}` must 401.

    Regression guard so the auth dependency stays on the router when the
    endpoint is refactored. Complements the unit-level test in
    `tests/api/test_predictions.py` by exercising the real middleware stack.
    """
    response = await live_app_client.get("/api/predictions/0000694720")
    assert response.status_code == 401, (
        f"Expected 401 without auth, got {response.status_code}: {response.text}"
    )
