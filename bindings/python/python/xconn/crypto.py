"""Shared Rust packet encryption. Peer-key trust is supplied by the application."""

from .messages import call


def generate_key_pair() -> tuple[bytes, bytes]:
    pair = call("crypto_key_pair")
    return pair["public"], pair["private"]


def shared_secret(private: bytes, public: bytes) -> bytes:
    return call("crypto_shared_secret", private, public)


def derive_key(secret: bytes, info: bytes) -> bytes:
    return call("crypto_derive_key", secret, info)


def encrypt(plaintext: bytes, key: bytes, additional_data: bytes = b"") -> bytes:
    return call("crypto_encrypt", plaintext, key, additional_data)


def decrypt(packet: bytes, key: bytes, additional_data: bytes = b"") -> bytes:
    return call("crypto_decrypt", packet, key, additional_data)
