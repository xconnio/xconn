use wampproto::authenticators::{cryptosign, wampcra};

#[test]
fn cryptosign_matches_existing_signature_vector_and_rejects_tampering() {
    let seed = hex::decode("c7e8c1f8f16ec37f53ed153f8afb7f18469b051f1d24dbea2097a2a104b2e9db").unwrap();
    let public_key = hex::decode("c53e4f2756a52ca1ed5cd00da108b3ed7bcffe6294e78283521e5102824f52d3").unwrap();
    let challenge = "a1d483092ec08960fedbaed2bc1d411568a59077b794210e251bd3abb1563f7c";
    let expected = concat!(
        "01d4b7a515b1023196e2bbb57c5202da72088f99a17eaeed62ba97ebf93381b92",
        "a3e8430154667e194d971fb41b090a9338b92021c39271e910a8ea072fe950c",
        "a1d483092ec08960fedbaed2bc1d411568a59077b794210e251bd3abb1563f7c"
    );
    assert_eq!(
        cryptosign::sign_cryptosign_challenge(challenge, &seed).unwrap(),
        expected
    );
    assert!(cryptosign::verify_cryptosign_signature(expected, &public_key));
    let mut tampered = expected.to_string();
    tampered.replace_range(0..2, "ff");
    assert!(!cryptosign::verify_cryptosign_signature(&tampered, &public_key));
    assert!(!cryptosign::verify_cryptosign_signature(
        &format!("{expected}00"),
        &public_key
    ));
    assert!(!cryptosign::verify_cryptosign_signature(expected, &[0; 32]));
    assert!(!cryptosign::verify_cryptosign_signature("zz", &public_key));
    assert!(!cryptosign::verify_cryptosign_signature("00", &public_key));
    assert!(!cryptosign::verify_cryptosign_signature(expected, &[0; 31]));
    assert!(cryptosign::sign_cryptosign_challenge("zz", &seed).is_err());
    assert!(cryptosign::sign_cryptosign_challenge(challenge, &[0; 31]).is_err());
}

#[test]
fn generated_keys_sign_and_verify_generated_challenges() {
    let (public_key, private_key) = cryptosign::generate_cryptosign_keypair().unwrap();
    let challenge = cryptosign::generate_cryptosign_challenge().unwrap();
    assert_eq!(hex::decode(&public_key).unwrap().len(), 32);
    assert_eq!(hex::decode(&private_key).unwrap().len(), 32);
    assert_eq!(hex::decode(&challenge).unwrap().len(), 32);
    assert_eq!(
        hex::decode(wampcra::generate_wampcra_nonce().unwrap()).unwrap().len(),
        16
    );
    let signed = cryptosign::sign_cryptosign_challenge(&challenge, &hex::decode(private_key).unwrap()).unwrap();
    assert!(cryptosign::verify_cryptosign_signature(
        &signed,
        &hex::decode(public_key).unwrap()
    ));
}

#[test]
fn wampcra_helpers_match_independent_hmac_and_pbkdf2_vectors() {
    // RFC 4231 HMAC-SHA256 test case 1, encoded as base64 for WAMP-CRA.
    let signature = "sDRMYdjbOFNcqK/OrwvxK4gdwgDJgz2nJuk3bC4yz/c=";
    assert_eq!(wampcra::sign_cra_challenge("Hi There", &[0x0b; 20]).unwrap(), signature);
    assert!(wampcra::verify_wampcra_signature(signature, "Hi There", &[0x0b; 20]));
    assert!(!wampcra::verify_wampcra_signature(signature, "tampered", &[0x0b; 20]));
    assert!(!wampcra::verify_wampcra_signature(
        "not base64",
        "Hi There",
        &[0x0b; 20]
    ));
    // PBKDF2-HMAC-SHA256("password", "salt", 1, 32).
    assert_eq!(
        wampcra::derive_wamp_cra_key("password", "salt", 1, 32).unwrap(),
        "Eg+2z/z4syxD5yJSVsT4N6hlSMkszDVICAWYfLcL4Xs="
    );
    assert!(wampcra::derive_wamp_cra_key("password", "salt", 0, 32).is_err());
    assert!(wampcra::derive_wamp_cra_key("password", "salt", 1, 0).is_err());
}

#[test]
fn wampcra_challenges_have_fresh_nonces_and_millisecond_utc_timestamps() {
    let first = wampcra::generate_wampcra_challenge(123, "alice", "user", "provider").unwrap();
    let second = wampcra::generate_wampcra_challenge(123, "alice", "user", "provider").unwrap();
    let first: serde_json::Value = serde_json::from_str(&first).unwrap();
    let second: serde_json::Value = serde_json::from_str(&second).unwrap();
    assert_ne!(first["nonce"], second["nonce"]);
    assert_eq!(hex::decode(first["nonce"].as_str().unwrap()).unwrap().len(), 16);
    assert_eq!(first["session"], 123);
    assert_eq!(first["authid"], "alice");
    assert_eq!(first["authrole"], "user");
    assert_eq!(first["authprovider"], "provider");
    assert_eq!(first["authmethod"], "wampcra");
    let timestamp = first["timestamp"].as_str().unwrap();
    assert_eq!(timestamp.len(), 24);
    assert!(timestamp.ends_with('Z'));
    let parsed = chrono::DateTime::parse_from_rfc3339(timestamp).unwrap();
    let now = chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now());
    assert!((now - parsed.with_timezone(&chrono::Utc)).num_seconds().abs() < 2);
}

#[test]
fn salted_wampcra_rejects_missing_parameters_instead_of_signing_unsalted() {
    use std::collections::HashMap;
    use wampproto::authenticators::authenticator::ClientAuthenticator;
    use wampproto::messages::{challenge::Challenge, types::Value};
    let authenticator = wampcra::WAMPCRAAuthenticator::new("alice", "secret", HashMap::new());
    let mut challenge = Challenge {
        auth_method: "wampcra".into(),
        extra: HashMap::from([
            ("challenge".into(), Value::Str("challenge".into())),
            ("salt".into(), Value::Str("salt".into())),
        ]),
    };
    assert!(
        authenticator
            .authenticate(&challenge)
            .unwrap_err()
            .message
            .starts_with("iterations string")
    );
    challenge.extra.insert("iterations".into(), Value::Int(1));
    assert!(
        authenticator
            .authenticate(&challenge)
            .unwrap_err()
            .message
            .starts_with("keylen string")
    );
    challenge.extra.insert("keylen".into(), Value::Int(32));
    let reply = authenticator.authenticate(&challenge).unwrap();
    let key = wampcra::derive_wamp_cra_key("secret", "salt", 1, 32).unwrap();
    assert_eq!(
        reply.signature,
        wampcra::sign_cra_challenge("challenge", key.as_bytes()).unwrap()
    );
}
