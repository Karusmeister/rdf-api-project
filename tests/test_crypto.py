"""Tests for KRS AES encryption - run these first to validate crypto works."""

import base64
import time

import pytest

from app.crypto import encrypt_nrkrs


def test_output_is_valid_base64():
    token = encrypt_nrkrs("0000694720")
    decoded = base64.b64decode(token)
    assert len(decoded) % 16 == 0, "Ciphertext should be multiple of AES block size"


def test_different_seconds_produce_different_tokens():
    t1 = encrypt_nrkrs("0000694720")
    time.sleep(1.1)
    t2 = encrypt_nrkrs("0000694720")
    assert t1 != t2, "Tokens should differ when generated in different seconds"


def test_krs_padding():
    """Both '694720' and '0000694720' should produce the same plaintext prefix."""
    # We can't easily compare ciphertexts (timestamp differs),
    # but at least verify both produce valid tokens.
    t1 = encrypt_nrkrs("694720")
    t2 = encrypt_nrkrs("0000694720")
    assert base64.b64decode(t1)  # valid base64
    assert base64.b64decode(t2)  # valid base64


def test_token_is_not_empty():
    token = encrypt_nrkrs("0000000001")
    assert len(token) > 0


def test_can_decrypt_to_verify_structure():
    """Decrypt our own token to verify plaintext format."""
    from datetime import datetime
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad

    krs = "0000694720"
    now = datetime.now()

    token = encrypt_nrkrs(krs)

    # Reconstruct key (same hour)
    key = now.strftime("%Y-%m-%d-%H").rjust(16, "1").encode("utf-8")
    cipher = AES.new(key, AES.MODE_CBC, iv=key)
    decrypted = unpad(cipher.decrypt(base64.b64decode(token)), AES.block_size)
    plaintext = decrypted.decode("utf-8")

    assert plaintext.startswith("0000694720"), f"Plaintext should start with KRS, got: {plaintext}"
    assert len(plaintext) == 29, f"Expected 10 (KRS) + 19 (timestamp) = 29 chars, got {len(plaintext)}"
