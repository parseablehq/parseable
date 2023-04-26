use aes_gcm::{aead::Aead, Aes256Gcm, KeyInit, Nonce};
use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::option::CONFIG;

pub fn key() -> String {
    format!(
        "{}:{}",
        CONFIG.parseable.username, CONFIG.parseable.password
    )
}

pub fn decrypt(key: &[u8], bytes: Bytes) -> Bytes {
    let noncebytes = &bytes[0..12];
    let nonce = Nonce::from_slice(noncebytes);
    let mut hasher = Sha256::new();
    hasher.update(key);
    let key = hasher.finalize();
    let cipher = Aes256Gcm::new_from_slice(&key).unwrap();
    let plaintext = cipher.decrypt(nonce, &bytes[12..]).unwrap();
    plaintext.into()
}

pub fn encrypt(key: &[u8], bytes: Bytes) -> Bytes {
    let noncebytes: [u8; 12] = rand::random::<[u8; 12]>();
    let mut hasher = Sha256::new();
    hasher.update(key);
    let key = hasher.finalize();
    let nonce = aes_gcm::Nonce::from_slice(&noncebytes);
    let cipher = Aes256Gcm::new_from_slice(&key).unwrap();
    let mut ciphertext = cipher.encrypt(nonce, &*bytes).unwrap();

    let mut encbuf = Vec::<u8>::with_capacity(noncebytes.len() + ciphertext.len());
    encbuf.extend_from_slice(&noncebytes);
    encbuf.append(&mut ciphertext);
    encbuf.into()
}
