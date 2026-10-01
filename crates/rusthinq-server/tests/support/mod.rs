use openssl::{
    hash::MessageDigest,
    pkey::{PKey, Private},
    rsa::Rsa,
    stack::Stack,
    x509::{
        X509NameBuilder, X509Req,
        extension::{BasicConstraints, SubjectAlternativeName},
    },
};
use rusthinq_server::certificates::Authority;
use std::sync::{Arc, OnceLock};

pub fn authority() -> Arc<Authority> {
    static AUTHORITY: OnceLock<Arc<Authority>> = OnceLock::new();
    AUTHORITY
        .get_or_init(|| Arc::new(Authority::generate("root.example", 2048).unwrap()))
        .clone()
}
pub fn csr() -> (Vec<u8>, PKey<Private>) {
    static CSR: OnceLock<(Vec<u8>, PKey<Private>)> = OnceLock::new();
    let (pem, key) = CSR.get_or_init(|| {
        let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
        let mut subject = X509NameBuilder::new().unwrap();
        subject
            .append_entry_by_text("CN", "requested-device")
            .unwrap();
        let mut csr = X509Req::builder().unwrap();
        csr.set_version(0).unwrap();
        csr.set_subject_name(&subject.build()).unwrap();
        csr.set_pubkey(&key).unwrap();
        let mut extensions = Stack::new().unwrap();
        extensions
            .push(
                SubjectAlternativeName::new()
                    .dns("untrusted.example")
                    .build(&csr.x509v3_context(None))
                    .unwrap(),
            )
            .unwrap();
        extensions
            .push(BasicConstraints::new().critical().ca().build().unwrap())
            .unwrap();
        csr.add_extensions(&extensions).unwrap();
        csr.sign(&key, MessageDigest::sha256()).unwrap();
        (csr.build().to_pem().unwrap(), key)
    });
    (pem.clone(), key.clone())
}
