"""
AES-128-CBC encryption of KRS numbers for the RDF wyszukiwanie endpoint.

Reverse-engineered from the live rdf-przegladarka.ms.gov.pl frontend bundle
(`main-7XB45WWR.js`, app version 3.0.11 dated 16.03.2026).

Algorithm:
  1. Generate a random 16-character decimal request key.
  2. Encrypt the zero-padded KRS using AES-CBC with that request key as key+IV.
  3. Concatenate "<inner_base64>.<request_key>".
  4. Encrypt that payload again with the static perm key as key+IV.
"""

import base64
import secrets

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

_PERM_KEY = b"6a5Qm4W&MkiD=hwo"


def _encrypt_aes_base64(plaintext: bytes, key: bytes) -> str:
    cipher = AES.new(key, AES.MODE_CBC, iv=key)
    encrypted = cipher.encrypt(pad(plaintext, AES.block_size))
    return base64.b64encode(encrypted).decode("utf-8")


def _random_request_key() -> str:
    """Generate a 16-digit decimal request key with full entropy."""
    return str(secrets.randbelow(9 * 10**15) + 10**15)


def encrypt_nrkrs(krs: str) -> str:
    """
    Encrypt a KRS number into a Base64 token expected by
    POST /dokumenty/wyszukiwanie.

    Must be called fresh for every request - the token embeds
    a random per-request key.
    """
    padded_krs = krs.zfill(10)
    request_key = _random_request_key()
    request_key_bytes = request_key.encode("utf-8")

    inner = _encrypt_aes_base64(padded_krs.encode("utf-8"), request_key_bytes)
    outer_payload = f"{inner}.{request_key}".encode("utf-8")
    return _encrypt_aes_base64(outer_payload, _PERM_KEY)
