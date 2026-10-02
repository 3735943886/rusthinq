//! LG pairing material and bounded native EC/CSR/OTP operations; no subprocesses.
use crate::cloud::{Client, Error, append, endpoint};
use base64::Engine;
use openssl::{
    ec::{EcGroup, EcKey},
    nid::Nid,
    pkey::{PKey, Private},
    rsa::Padding,
    x509::{X509, X509NameBuilder, X509Req},
};
use reqwest::{Method, header::HeaderMap};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{fmt, time::Duration};
use url::Url;
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "platform", rename_all = "lowercase", deny_unknown_fields)]
pub enum Material {
    ThinQ1 {
        http_server: String,
        rti_server: String,
    },
    ThinQ2 {
        country: String,
        api_server: String,
        mqtt_server: String,
        ca_certificate: String,
        private_key: String,
        certificate: String,
        pub_topic: String,
        prov_topic: String,
        sub_topic: String,
    },
}
impl fmt::Debug for Material {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PairingMaterial(<redacted>)")
    }
}
pub(crate) fn service(value: &str) -> Result<Url, Error> {
    if value.len() > 4096 {
        return Err(Error::InvalidResponse);
    }
    let value = if value.contains("://") {
        value.to_owned()
    } else {
        format!("ssl://{value}")
    };
    let url = Url::parse(&value).map_err(|_| Error::InvalidResponse)?;
    if !matches!(url.scheme(), "ssl" | "mqtts")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || !matches!(url.path(), "" | "/")
        || url.query().is_some()
        || url.fragment().is_some()
        || url.port() == Some(0)
    {
        return Err(Error::InvalidResponse);
    }
    Ok(url)
}
fn topic(value: &str, filter: bool) -> Result<(), Error> {
    if value.is_empty()
        || value.len() > 1024
        || value.chars().any(char::is_control)
        || if filter {
            !rusthinq_protocol::mqtt::valid_filter(value)
        } else {
            value.contains(['#', '+'])
        }
    {
        return Err(Error::InvalidResponse);
    }
    Ok(())
}
fn certificates(value: &str) -> Result<Vec<X509>, Error> {
    if value.len() > 65536 {
        return Err(Error::InvalidResponse);
    }
    let certificates =
        X509::stack_from_pem(value.as_bytes()).map_err(|_| Error::InvalidResponse)?;
    if certificates.is_empty() || certificates.len() > 16 {
        return Err(Error::InvalidResponse);
    }
    Ok(certificates)
}
fn identity(key: &str, leaf: &str, ca: &str, current: bool) -> Result<(), Error> {
    if key.len() > 8192 {
        return Err(Error::InvalidResponse);
    }
    let key = PKey::private_key_from_pem(key.as_bytes()).map_err(|_| Error::InvalidResponse)?;
    let chain = certificates(leaf)?;
    certificates(ca)?;
    if !chain[0]
        .public_key()
        .map_err(|_| Error::InvalidResponse)?
        .public_eq(&key)
    {
        return Err(Error::InvalidResponse);
    }
    // The regional root verifies the MQTT server. AWS IoT may register a
    // client certificate issued by a different CA; do not conflate these roots.
    let now = openssl::asn1::Asn1Time::days_from_now(0).map_err(|_| Error::Crypto)?;
    if current
        && (chain[0]
            .not_before()
            .compare(&now)
            .map_err(|_| Error::InvalidResponse)?
            == std::cmp::Ordering::Greater
            || chain[0]
                .not_after()
                .compare(&now)
                .map_err(|_| Error::InvalidResponse)?
                != std::cmp::Ordering::Greater)
    {
        return Err(Error::InvalidResponse);
    }
    Ok(())
}
impl Material {
    /// Revalidate deserialized material before any credential is installed in a transport.
    pub fn validate(&self) -> Result<(), Error> {
        self.check(true)
    }
    /// Structural recovery preserves expired credentials for explicit re-pairing.
    pub fn validate_stored(&self) -> Result<(), Error> {
        self.check(false)
    }
    fn check(&self, current: bool) -> Result<(), Error> {
        match self {
            Self::ThinQ1 {
                http_server,
                rti_server,
            } => {
                endpoint(http_server)?;
                service(rti_server)?;
            }
            Self::ThinQ2 {
                country,
                api_server,
                mqtt_server,
                ca_certificate,
                private_key,
                certificate,
                pub_topic,
                prov_topic,
                sub_topic,
            } => {
                if country.len() != 2 || !country.bytes().all(|b| b.is_ascii_uppercase()) {
                    return Err(Error::InvalidResponse);
                }
                endpoint(api_server)?;
                service(mqtt_server)?;
                topic(pub_topic, false)?;
                topic(prov_topic, false)?;
                topic(sub_topic, true)?;
                identity(private_key, certificate, ca_certificate, current)?;
            }
        };
        Ok(())
    }
}
struct Crypto {
    private: String,
    public: String,
    csr: String,
    certificate_ciphertext: String,
    registration_ciphertext: String,
}
fn encrypt(key: &str, plain: &[u8]) -> Result<Vec<u8>, Error> {
    if key.len() > 8192 {
        return Err(Error::InvalidInput);
    }
    let public = if key.contains("BEGIN") {
        PKey::public_key_from_pem(key.as_bytes())
    } else {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(key.trim())
            .map_err(|_| Error::InvalidInput)?;
        PKey::public_key_from_der(&bytes)
    }
    .map_err(|_| Error::InvalidInput)?;
    let rsa = public.rsa().map_err(|_| Error::InvalidInput)?;
    if !matches!(rsa.size(), 256 | 384 | 512) {
        return Err(Error::InvalidInput);
    }
    let mut ciphertext = vec![0; rsa.size() as usize];
    let n = rsa
        .public_encrypt(plain, &mut ciphertext, Padding::PKCS1)
        .map_err(|_| Error::Crypto)?;
    ciphertext.truncate(n);
    Ok(ciphertext)
}
fn prepare(id: &str, otp: &str, key: &str) -> Result<Crypto, Error> {
    if id.is_empty()
        || id.len() > 256
        || id.chars().any(char::is_control)
        || otp.is_empty()
        || otp.len() > 397
        || otp.chars().any(char::is_control)
    {
        return Err(Error::InvalidInput);
    }
    let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).map_err(|_| Error::Crypto)?;
    let private: PKey<Private> =
        PKey::from_ec_key(EcKey::generate(&group).map_err(|_| Error::Crypto)?)
            .map_err(|_| Error::Crypto)?;
    let public = String::from_utf8(private.public_key_to_pem().map_err(|_| Error::Crypto)?)
        .map_err(|_| Error::Crypto)?;
    let mut name = X509NameBuilder::new().map_err(|_| Error::Crypto)?;
    for (k, v) in [("CN", "*.clip.com"), ("O", "LGE"), ("C", "KR")] {
        name.append_entry_by_text(k, v).map_err(|_| Error::Crypto)?;
    }
    let mut csr = X509Req::builder().map_err(|_| Error::Crypto)?;
    csr.set_subject_name(&name.build())
        .map_err(|_| Error::Crypto)?;
    csr.set_pubkey(&private).map_err(|_| Error::Crypto)?;
    csr.sign(&private, openssl::hash::MessageDigest::sha256())
        .map_err(|_| Error::Crypto)?;
    let csr = String::from_utf8(csr.build().to_pem().map_err(|_| Error::Crypto)?)
        .map_err(|_| Error::Crypto)?;
    let mut nonce = [0; 8];
    openssl::rand::rand_bytes(&mut nonce).map_err(|_| Error::Crypto)?;
    let mut plain = nonce.to_vec();
    plain.extend_from_slice(otp.as_bytes());
    for text in [id, csr.as_str(), public.as_str()] {
        plain.extend_from_slice(&openssl::sha::sha256(text.as_bytes()));
    }
    let certificate_ciphertext =
        base64::engine::general_purpose::STANDARD.encode(encrypt(key, &plain)?);
    let mut plain = nonce.to_vec();
    plain.extend_from_slice(&openssl::sha::sha256(id.as_bytes()));
    let registration_ciphertext =
        base64::engine::general_purpose::STANDARD.encode(encrypt(key, &plain)?);
    Ok(Crypto {
        private: String::from_utf8(
            private
                .private_key_to_pem_pkcs8()
                .map_err(|_| Error::Crypto)?,
        )
        .map_err(|_| Error::Crypto)?,
        public,
        csr,
        certificate_ciphertext,
        registration_ciphertext,
    })
}
pub(crate) struct ResultMaterial {
    pub material: Material,
    pub registration_ciphertext: String,
}
async fn iot(
    client: &Client,
    url: Url,
    headers: HeaderMap,
    body: Option<Value>,
) -> Result<Value, Error> {
    let raw = client
        .request(
            url,
            if body.is_some() {
                Method::POST
            } else {
                Method::GET
            },
            headers,
            body.map(|body| body.to_string()),
        )
        .await?;
    if let Some(code) = raw.get("resultCode")
        && code != "0000"
    {
        return Err(Error::InvalidResponse);
    }
    Ok(raw.get("result").cloned().unwrap_or(raw))
}
pub(crate) async fn pair_at(
    client: &Client,
    route: Url,
    country: &str,
    id: &str,
    otp: &str,
    key: &str,
) -> Result<ResultMaterial, Error> {
    let crypto = prepare(id, otp, key)?;
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-country-code",
        country.parse().map_err(|_| Error::InvalidInput)?,
    );
    headers.insert("x-service-phase", "OP".parse().expect("constant"));
    let route = tokio::time::timeout(
        Duration::from_secs(5),
        iot(client, append(&route, "route")?, headers, None),
    )
    .await
    .map_err(|_| Error::Network)??;
    let api = endpoint(route["apiServer"].as_str().ok_or(Error::InvalidResponse)?)?;
    let mqtt = route["mqttServer"]
        .as_str()
        .ok_or(Error::InvalidResponse)?
        .to_owned();
    service(&mqtt)?;
    let mut ca_url = append(&api, "route/certificate")?;
    ca_url.query_pairs_mut().append_pair("name", "aws-iot");
    let ca = iot(client, ca_url, HeaderMap::new(), None).await?;
    let ca = ca["certificatePem"]
        .as_str()
        .ok_or(Error::InvalidResponse)?
        .to_owned();
    certificates(&ca)?;
    let mut certificate_url = api.clone();
    certificate_url
        .path_segments_mut()
        .map_err(|_| Error::InvalidResponse)?
        .pop_if_empty()
        .push("device")
        .push(id)
        .push("certificate");
    let mut headers = HeaderMap::new();
    headers.insert("x-provide-type", "immediate".parse().expect("constant"));
    headers.insert(
        "content-type",
        "application/json".parse().expect("constant"),
    );
    let result=iot(client,certificate_url,headers,Some(json!({"otp":otp,"csr":crypto.csr,"publickey":crypto.public,"ciphertext":crypto.certificate_ciphertext}))).await?;
    let material = Material::ThinQ2 {
        country: country.into(),
        api_server: api.into(),
        mqtt_server: mqtt,
        ca_certificate: ca,
        private_key: crypto.private,
        certificate: result["certificatePem"]
            .as_str()
            .ok_or(Error::InvalidResponse)?
            .into(),
        pub_topic: result
            .pointer("/publication/message")
            .and_then(Value::as_str)
            .ok_or(Error::InvalidResponse)?
            .into(),
        prov_topic: result
            .pointer("/publication/provisioning")
            .and_then(Value::as_str)
            .ok_or(Error::InvalidResponse)?
            .into(),
        sub_topic: result
            .pointer("/subscription/message")
            .and_then(Value::as_str)
            .ok_or(Error::InvalidResponse)?
            .into(),
    };
    material.validate()?;
    Ok(ResultMaterial {
        material,
        registration_ciphertext: crypto.registration_ciphertext,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn otp_envelopes_decrypt_to_the_reference_nonce_and_hashes() {
        let rsa = openssl::rsa::Rsa::generate(2048).unwrap();
        let pem = String::from_utf8(rsa.public_key_to_pem().unwrap()).unwrap();
        let crypto = prepare("device", "otp", &pem).unwrap();
        let mut first = vec![0; 256];
        let n = rsa
            .private_decrypt(
                &base64::engine::general_purpose::STANDARD
                    .decode(&crypto.certificate_ciphertext)
                    .unwrap(),
                &mut first,
                Padding::PKCS1,
            )
            .unwrap();
        first.truncate(n);
        let mut second = vec![0; 256];
        let n = rsa
            .private_decrypt(
                &base64::engine::general_purpose::STANDARD
                    .decode(&crypto.registration_ciphertext)
                    .unwrap(),
                &mut second,
                Padding::PKCS1,
            )
            .unwrap();
        second.truncate(n);
        assert_eq!(&first[..8], &second[..8]);
        assert_eq!(&first[8..11], b"otp");
        assert_eq!(&first[11..43], &openssl::sha::sha256(b"device"));
        assert_eq!(&first[43..75], &openssl::sha::sha256(crypto.csr.as_bytes()));
        assert_eq!(
            &first[75..],
            &openssl::sha::sha256(crypto.public.as_bytes())
        );
        assert_eq!(&second[8..], &openssl::sha::sha256(b"device"));
        let csr = X509Req::from_pem(crypto.csr.as_bytes()).unwrap();
        let public = PKey::public_key_from_pem(crypto.public.as_bytes()).unwrap();
        assert!(csr.verify(&public).unwrap());
        assert!(public.public_eq(&PKey::private_key_from_pem(crypto.private.as_bytes()).unwrap()));
        let der =
            base64::engine::general_purpose::STANDARD.encode(rsa.public_key_to_der().unwrap());
        assert!(prepare("device", "otp", &der).is_ok());
    }
    #[test]
    fn invalid_material_and_unbounded_crypto_inputs_are_rejected() {
        assert!(prepare("", "otp", "bad").is_err());
        assert!(prepare("d", &"o".repeat(398), "bad").is_err());
        assert!(prepare("d", "o", &"x".repeat(8193)).is_err());
        let material = Material::ThinQ1 {
            http_server: "http://cloud.example".into(),
            rti_server: "host:5222".into(),
        };
        assert!(material.validate().is_err());
        let material = Material::ThinQ1 {
            http_server: "https://cloud.example".into(),
            rti_server: "host:5222".into(),
        };
        material.validate().unwrap();
        assert_eq!(format!("{material:?}"), "PairingMaterial(<redacted>)");
        let restored: Material =
            serde_json::from_slice(&serde_json::to_vec(&material).unwrap()).unwrap();
        restored.validate().unwrap();
        for value in [
            "ssl://user:pass@broker.example",
            "ssl://broker.example?x=y",
            "ssl://broker.example/path",
            "ssl://broker.example:0",
            "http://broker.example",
        ] {
            assert!(service(value).is_err());
        }
        assert!(topic("device/+/set", true).is_ok());
        assert!(topic("device/#/set", true).is_err());
        assert!(topic("device/+", false).is_err());
    }
}
