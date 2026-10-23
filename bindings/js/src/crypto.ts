import { getNative, rustCall } from "./native.js";
import { ProtocolError } from "./exception.js";

interface PacketCryptoApi {
    key_pair(): { public: Uint8Array; private: Uint8Array };
    shared_secret(privateKey: Uint8Array, publicKey: Uint8Array): Uint8Array;
    derive_key(secret: Uint8Array, info: Uint8Array): Uint8Array;
    encrypt(plaintext: Uint8Array, key: Uint8Array, additionalData: Uint8Array): Uint8Array;
    decrypt(packet: Uint8Array, key: Uint8Array, additionalData: Uint8Array): Uint8Array;
}

function packetCrypto(): PacketCryptoApi {
    const api = (getNative() as unknown as { PacketCrypto?: PacketCryptoApi }).PacketCrypto;
    if (!api) throw new ProtocolError("Packet encryption is not included in this WASM build");
    return api;
}

/** Peer-key trust is supplied by the application. All cryptography runs in Rust/WASM. */
export function generateKeyPair(): { publicKey: Uint8Array; privateKey: Uint8Array } {
    const pair = rustCall(() => packetCrypto().key_pair()) as { public: Uint8Array; private: Uint8Array };
    return { publicKey: pair.public, privateKey: pair.private };
}

export function sharedSecret(privateKey: Uint8Array, publicKey: Uint8Array): Uint8Array {
    return rustCall(() => packetCrypto().shared_secret(privateKey, publicKey));
}

export function deriveKey(secret: Uint8Array, info: Uint8Array): Uint8Array {
    return rustCall(() => packetCrypto().derive_key(secret, info));
}

export function encrypt(plaintext: Uint8Array, key: Uint8Array, additionalData = new Uint8Array()): Uint8Array {
    return rustCall(() => packetCrypto().encrypt(plaintext, key, additionalData));
}

export function decrypt(packet: Uint8Array, key: Uint8Array, additionalData = new Uint8Array()): Uint8Array {
    return rustCall(() => packetCrypto().decrypt(packet, key, additionalData));
}
