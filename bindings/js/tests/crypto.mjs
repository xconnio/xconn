export function exerciseCrypto(module, fixture) {
    const data = (key) => Uint8Array.from(fixture[key].match(/../g), (byte) => Number.parseInt(byte, 16));
    const same = (actual, expected) => {
        if (actual.length !== expected.length || actual.some((byte, index) => byte !== expected[index])) {
            throw new Error("Packet cryptography differs from the independent Go implementation");
        }
    };
    const secret = module.crypto.sharedSecret(data("private"), data("public"));
    same(secret, data("shared"));
    const key = module.crypto.deriveKey(secret, data("info"));
    same(key, data("key"));
    same(module.crypto.decrypt(data("packet"), key, data("aad")), data("plaintext"));
    const packet = module.crypto.encrypt(data("plaintext"), key, data("aad"));
    same(module.crypto.decrypt(packet, key, data("aad")), data("plaintext"));
    let rejected = false;
    try {
        module.crypto.decrypt(packet, key);
    } catch {
        rejected = true;
    }
    if (!rejected) throw new Error("Packet authentication accepted the wrong context");
    const pair = module.crypto.generateKeyPair();
    if (pair.publicKey.length !== 32 || pair.privateKey.length !== 32) throw new Error("Invalid X25519 key pair");
    return "passed";
}
