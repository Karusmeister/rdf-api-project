import pytest


def pytest_addoption(parser):
    parser.addoption("--e2e", action="store_true", default=False, help="Run end-to-end tests (requires network)")


def pytest_configure(config):
    config.addinivalue_line("markers", "e2e: end-to-end test requiring network access")
    config.addinivalue_line("markers", "regression: live upstream regression coverage")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--e2e"):
        skip_e2e = pytest.mark.skip(reason="Need --e2e flag to run")
        for item in items:
            if "e2e" in item.keywords:
                item.add_marker(skip_e2e)
