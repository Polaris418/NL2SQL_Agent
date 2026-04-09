from __future__ import annotations

import base64
import hashlib
import os
import secrets
from dataclasses import dataclass


@dataclass(slots=True)
class PasswordCipher:
    """Encrypts connection passwords before storing them in metadata."""

    master_key: str

    @classmethod
    def from_env(cls) -> "PasswordCipher":
        master_key = os.getenv("NL2SQL_MASTER_KEY", "change-me-in-production")
        return cls(master_key=master_key)

    def _derive_fernet_key(self) -> bytes:
        digest = hashlib.sha256(self.master_key.encode("utf-8")).digest()
        return base64.urlsafe_b64encode(digest)

    def encrypt(self, plaintext: str | None) -> str:
        if plaintext in (None, ""):
            return ""

        payload = plaintext.encode("utf-8")
        try:
            from cryptography.fernet import Fernet  # type: ignore

            token = Fernet(self._derive_fernet_key()).encrypt(payload)
            return token.decode("utf-8")
        except Exception:
            salt = secrets.token_bytes(16)
            keystream = self._keystream(salt, len(payload))
            ciphertext = bytes(a ^ b for a, b in zip(payload, keystream))
            return base64.urlsafe_b64encode(salt + ciphertext).decode("utf-8")

    def decrypt(self, token: str | None) -> str:
        if token in (None, ""):
            return ""

        try:
            from cryptography.fernet import Fernet  # type: ignore

            payload = Fernet(self._derive_fernet_key()).decrypt(token.encode("utf-8"))
            return payload.decode("utf-8")
        except Exception:
            raw = base64.urlsafe_b64decode(token.encode("utf-8"))
            salt, ciphertext = raw[:16], raw[16:]
            keystream = self._keystream(salt, len(ciphertext))
            plaintext = bytes(a ^ b for a, b in zip(ciphertext, keystream))
            return plaintext.decode("utf-8")

    def _keystream(self, salt: bytes, length: int) -> bytes:
        seed = self.master_key.encode("utf-8")
        output = bytearray()
        counter = 0
        while len(output) < length:
            block = hashlib.pbkdf2_hmac(
                "sha256",
                seed,
                salt + counter.to_bytes(4, "big"),
                100_000,
                dklen=32,
            )
            output.extend(block)
            counter += 1
        return bytes(output[:length])

