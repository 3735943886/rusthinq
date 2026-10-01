use rusthinq_protocol::client_hello::{Error, Hello, hostname, parse};

fn handshake(name: Option<&str>) -> Vec<u8> {
    let mut body = vec![3, 3];
    body.extend_from_slice(&[0; 32]);
    body.extend_from_slice(&[0, 0, 2, 0, 47, 1, 0]);
    if let Some(name) = name {
        let mut sni = ((name.len() + 3) as u16).to_be_bytes().to_vec();
        sni.push(0);
        sni.extend_from_slice(&(name.len() as u16).to_be_bytes());
        sni.extend_from_slice(name.as_bytes());
        let mut extensions = vec![0, 0];
        extensions.extend_from_slice(&(sni.len() as u16).to_be_bytes());
        extensions.extend_from_slice(&sni);
        body.extend_from_slice(&(extensions.len() as u16).to_be_bytes());
        body.extend_from_slice(&extensions);
    }
    let mut result = vec![1];
    result.extend_from_slice(&(body.len() as u32).to_be_bytes()[1..]);
    result.extend_from_slice(&body);
    result
}
fn record(handshake: &[u8]) -> Vec<u8> {
    let mut bytes = vec![22, 3, 1];
    bytes.extend_from_slice(&(handshake.len() as u16).to_be_bytes());
    bytes.extend_from_slice(handshake);
    bytes
}
#[test]
fn every_byte_and_record_split_preserves_sni() {
    let handshake = handshake(Some("kic-common.lgthinq.com"));
    let expected = Hello::Ready(Some("kic-common.lgthinq.com".into()));
    let bytes = record(&handshake);
    for cut in 0..bytes.len() {
        assert_eq!(parse(&bytes[..cut], 65536), Ok(Hello::Incomplete));
    }
    assert_eq!(parse(&bytes, 65536), Ok(expected.clone()));
    for split in 1..handshake.len() {
        let mut fragmented = record(&handshake[..split]);
        fragmented.extend_from_slice(&record(&handshake[split..]));
        assert_eq!(parse(&fragmented, 65536), Ok(expected.clone()));
    }
    assert_eq!(
        parse(&record(&self::handshake(None)), 65536),
        Ok(Hello::Ready(None))
    );
    assert_eq!(
        parse(&record(&self::handshake(Some("LOCAL.Example"))), 65536),
        Ok(Hello::Ready(Some("local.example".into())))
    );
}
#[test]
fn malformed_lengths_bounds_and_hostnames_are_rejected() {
    let bytes = record(&handshake(Some("local.example")));
    assert_eq!(parse(&bytes, bytes.len() - 1), Err(Error::Exceeded));
    for name in [
        "",
        "bad name",
        "127.0.0.1",
        "host..example",
        "*.example",
        "host/example",
        "-bad.example",
    ] {
        assert!(!hostname(name));
        assert_eq!(
            parse(&record(&handshake(Some(name))), 65536),
            Err(Error::Invalid)
        );
    }
    assert!(!hostname(&format!("{}.example", "a".repeat(64))));
    for offset in 0..bytes.len() {
        for replacement in [0, 1, 127, 255] {
            let mut changed = bytes.clone();
            changed[offset] = replacement;
            for cut in [offset, offset + 1, bytes.len()] {
                let _ = parse(&changed[..cut], 65536);
            }
        }
    }
    let mut malformed = bytes.clone();
    malformed[0] = 23;
    assert_eq!(parse(&malformed, 65536), Err(Error::Invalid));
    let mut malformed = bytes.clone();
    malformed[6..9].copy_from_slice(&[255, 255, 255]);
    assert_eq!(parse(&malformed, 65536), Err(Error::Exceeded));
    // Duplicate SNI extensions with otherwise consistent record/handshake/vector lengths.
    let mut duplicated = handshake(Some("local.example"));
    let extension = duplicated[47..].to_vec();
    duplicated.extend_from_slice(&extension);
    let length = (duplicated.len() - 4) as u32;
    duplicated[1..4].copy_from_slice(&length.to_be_bytes()[1..]);
    duplicated[45..47].copy_from_slice(&((extension.len() * 2) as u16).to_be_bytes());
    assert_eq!(parse(&record(&duplicated), 65536), Err(Error::Invalid));
}
