from __future__ import annotations

import asyncio
import time

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.auth import create_token
from app.db import connection as db_conn
from app.db import prediction_db
from app.main import app


class _FakeRawConn:
    def __init__(self):
        self.autocommit = False


class _FakePool:
    def __init__(self):
        self.borrowed = 0
        self.max_borrowed = 0
        self.get_calls = 0
        self.put_calls = 0

    def getconn(self):
        self.get_calls += 1
        self.borrowed += 1
        if self.borrowed > self.max_borrowed:
            self.max_borrowed = self.borrowed
        return _FakeRawConn()

    def putconn(self, _raw):
        self.put_calls += 1
        self.borrowed -= 1


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest_asyncio.fixture
async def client(monkeypatch):
    from app import rdf_client

    monkeypatch.setattr(rdf_client, "_client", object())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_request_pooling_under_concurrency(client, monkeypatch):
    """Stress middleware-managed per-request DB pooling under concurrent requests."""
    fake_pool = _FakePool()
    monkeypatch.setattr(db_conn, "_pool", fake_pool)

    user = {
        "id": "user-1",
        "email": "test@example.com",
        "name": "Test User",
        "auth_method": "local",
        "password_hash": "hash",
        "is_verified": True,
        "has_full_access": True,
        "is_active": True,
        "created_at": "2026-01-01",
        "last_login_at": None,
    }

    def slow_get_user_by_id(_user_id: str):
        # Keep each request in-flight briefly to force overlap.
        time.sleep(0.02)
        return user

    monkeypatch.setattr(prediction_db, "get_user_by_id", slow_get_user_by_id)
    monkeypatch.setattr(prediction_db, "get_user_krs_access", lambda _uid: [])

    headers = {"Authorization": f"Bearer {create_token(user['id'], user['email'])}"}

    async def _call_me():
        resp = await client.get("/api/auth/me", headers=headers)
        assert resp.status_code == 200

    n = 50
    await asyncio.gather(*(_call_me() for _ in range(n)))

    assert fake_pool.get_calls == n
    assert fake_pool.put_calls == n
    assert fake_pool.borrowed == 0
    assert fake_pool.max_borrowed > 1
    assert db_conn._request_conn.get() is None
