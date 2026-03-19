"""
AES-128-CBC encryption of KRS numbers for the RDF wyszukiwanie endpoint.

Reverse-engineered from rdf-przegladarka.ms.gov.pl frontend JS
(main-C7XHMT4M.js, function encryptNrKrs).

Algorithm:
  plaintext = krs.zfill(10) + now.strftime("%Y-%m-%d-%H-%M-%S")
  key = iv  = now.strftime("%Y-%m-%d-%H").rjust(16, "1")
  token     = base64( AES-CBC(plaintext, key, iv, PKCS7) )
"""

import base64
from datetime import datetime

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad


def encrypt_nrkrs(krs: str) -> str:
    """
    Encrypt a KRS number into a Base64 token expected by
    POST /dokumenty/wyszukiwanie.

    Must be called fresh for every request - the token includes
    the current timestamp down to the second.
    """
    now = datetime.now()

    # Plaintext: zero-padded KRS + full timestamp with seconds
    timestamp_full = now.strftime("%Y-%m-%d-%H-%M-%S")
    plaintext = krs.zfill(10) + timestamp_full

    # Key and IV are identical: hour-level timestamp padded to 16 bytes
    timestamp_hour = now.strftime("%Y-%m-%d-%H")
    key = timestamp_hour.rjust(16, "1").encode("utf-8")

    cipher = AES.new(key, AES.MODE_CBC, iv=key)
    encrypted = cipher.encrypt(pad(plaintext.encode("utf-8"), AES.block_size))
    return base64.b64encode(encrypted).decode("utf-8")
