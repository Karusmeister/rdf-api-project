"""Tests for RDF KRS encryption used by the live wyszukiwanie endpoint."""

import base64
from unittest.mock import patch

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

from app.crypto import _PERM_KEY, encrypt_nrkrs


def test_output_is_valid_base64():
    token = encrypt_nrkrs("0000694720")
    decoded = base64.b64decode(token)
    assert len(decoded) % 16 == 0, "Ciphertext should be multiple of AES block size"


def test_different_calls_produce_different_tokens():
    t1 = encrypt_nrkrs("0000694720")
    t2 = encrypt_nrkrs("0000694720")
    assert t1 != t2, "Tokens should differ when generated with different random request keys"


def test_krs_padding():
    """Both short and zero-padded KRS inputs should encrypt successfully."""
    assert base64.b64decode(encrypt_nrkrs("694720"))
    assert base64.b64decode(encrypt_nrkrs("0000694720"))


def test_token_is_not_empty():
    token = encrypt_nrkrs("0000000001")
    assert len(token) > 0


def test_can_decrypt_to_verify_structure():
    krs = "0000694720"
    request_key = "0000123456789012"

    with patch("app.crypto._random_request_key", return_value=request_key):
        token = encrypt_nrkrs(krs)

    perm_cipher = AES.new(_PERM_KEY, AES.MODE_CBC, iv=_PERM_KEY)
    outer_plaintext = unpad(perm_cipher.decrypt(base64.b64decode(token)), AES.block_size).decode("utf-8")
    inner_base64, outer_request_key = outer_plaintext.split(".", 1)

    assert outer_request_key == request_key

    request_key_bytes = request_key.encode("utf-8")
    inner_cipher = AES.new(request_key_bytes, AES.MODE_CBC, iv=request_key_bytes)
    plaintext = unpad(inner_cipher.decrypt(base64.b64decode(inner_base64)), AES.block_size).decode("utf-8")

    assert plaintext == krs
