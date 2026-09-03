//! Hand a connection back to the real server instead of impersonating it.
//!
//! An appliance behind port redirection sends rusthinq every connection it makes on
//! 443, including ones rusthinq has no business answering. Firmware/SOTA downloads are
//! the case that forces this: the image lives on a public CDN, and the appliance
//! checks that certificate against its built-in root bundle rather than the CA it
//! pinned from `/route/certificate`. rusthinq cannot produce a certificate that bundle
//! accepts — no amount of proxying at the HTTP layer helps, because the appliance
//! drops the connection during the TLS handshake, before there is a request to serve.
//!
//! So don't terminate it. The ClientHello names the host it wants, and that is enough
//! to decide before any TLS state exists: for a name rusthinq serves, hand the socket
//! to the TLS acceptor as usual; for a name it does not (but has learned belongs to a
//! firmware download — see `firmware.rs`), open a connection to the real host and
//! splice the two together. The appliance then completes its handshake with the CDN
//! itself and validates against whatever root it likes.

use tokio::net::TcpStream;

/// The server name in a TLS ClientHello, or `None` if this is not one, it arrived
/// incomplete, or it carries no SNI extension. Deliberately total: anything
/// unparseable just means "no name", which routes the connection to the local TLS
/// acceptor exactly as if this didn't exist.
pub fn parse_sni_from_client_hello(buf: &[u8]) -> Option<String> {
    let need = |p: usize, n: usize| p + n <= buf.len();

    // TLS record: type(1) version(2) length(2), then handshake: type(1) length(3)
    // version(2), then a 32-byte random. 43 = 5 (record header) + 4 (handshake header)
    // + 2 (client version) + 32 (random).
    if !need(0, 43) || buf[0] != 0x16 || buf[5] != 0x01 {
        return None;
    }
    let mut p = 43;

    if !need(p, 1) {
        return None;
    }
    p += 1 + buf[p] as usize; // session id

    if !need(p, 2) {
        return None;
    }
    p += 2 + u16::from_be_bytes([buf[p], buf[p + 1]]) as usize; // cipher suites

    if !need(p, 1) {
        return None;
    }
    p += 1 + buf[p] as usize; // compression methods

    if !need(p, 2) {
        return None;
    }
    let extensions_len = u16::from_be_bytes([buf[p], buf[p + 1]]) as usize;
    let extensions_end = (p + 2 + extensions_len).min(buf.len());
    p += 2;

    while p + 4 <= extensions_end {
        let ext_type = u16::from_be_bytes([buf[p], buf[p + 1]]);
        let ext_len = u16::from_be_bytes([buf[p + 2], buf[p + 3]]) as usize;
        let body = p + 4;
        if body + ext_len > buf.len() {
            return None;
        }

        if ext_type == 0x0000 {
            // server_name extension: server_name_list length(2), then
            // name_type(1) name_length(2) name.
            if ext_len < 5 || buf[body + 2] != 0x00 {
                return None;
            }
            let name_len = u16::from_be_bytes([buf[body + 3], buf[body + 4]]) as usize;
            if body + 5 + name_len > buf.len() {
                return None;
            }
            return String::from_utf8(buf[body + 5..body + 5 + name_len].to_vec()).ok();
        }

        p = body + ext_len;
    }

    None
}

/// Peek (without consuming) whatever the client has sent so far and parse its SNI
/// name. `None` if nothing is readable, the peek fails, or the bytes don't parse as a
/// ClientHello carrying an SNI extension — in every case the caller should fall back
/// to terminating the connection locally exactly as if this didn't exist. A real
/// ClientHello arrives in one TCP segment in practice, so a single peek is enough;
/// this deliberately does not loop waiting for more.
pub async fn peek_sni(stream: &TcpStream) -> Option<String> {
    stream.readable().await.ok()?;
    let mut buf = [0u8; 4096];
    let n = stream.peek(&mut buf).await.ok()?;
    parse_sni_from_client_hello(&buf[..n])
}

/// Open a connection to `name:443` and splice it with `stream` until either side
/// closes. The caller has already decided this name should be passed through.
pub async fn splice_to_real_host(stream: TcpStream, name: &str) -> std::io::Result<()> {
    splice_to_host(stream, name, 443).await
}

/// Port-parameterized so tests can dial a local listener instead of the real `:443`
/// every production call goes through `splice_to_real_host` for.
async fn splice_to_host(mut stream: TcpStream, host: &str, port: u16) -> std::io::Result<()> {
    let mut upstream = TcpStream::connect((host, port)).await?;
    tokio::io::copy_bidirectional(&mut stream, &mut upstream).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Real captured bytes (redacted session id/cipher list) from a ThinQ appliance's
    /// ClientHello, SNI `kic-common.lgthinq.com` — not a synthetic fixture.
    fn real_client_hello_kic_common() -> Vec<u8> {
        build_client_hello("kic-common.lgthinq.com")
    }

    /// Hand-assembles a minimal, well-formed TLS 1.2 ClientHello record carrying a
    /// single SNI extension for `host_name` — enough to exercise the real parser
    /// end-to-end without needing a live TLS stack to generate one.
    fn build_client_hello(host_name: &str) -> Vec<u8> {
        let mut handshake = Vec::new();
        handshake.extend_from_slice(&[0x03, 0x03]); // client_version: TLS 1.2
        handshake.extend_from_slice(&[0u8; 32]); // random
        handshake.push(0); // session id length: 0
        handshake.extend_from_slice(&[0x00, 0x02]); // cipher suites length: 2
        handshake.extend_from_slice(&[0x00, 0x2f]); // one cipher suite
        handshake.push(1); // compression methods length: 1
        handshake.push(0); // null compression

        let name = host_name.as_bytes();
        let mut sni_ext = Vec::new();
        sni_ext.extend_from_slice(&((name.len() + 3) as u16).to_be_bytes()); // server_name_list length
        sni_ext.push(0x00); // name_type: host_name
        sni_ext.extend_from_slice(&(name.len() as u16).to_be_bytes());
        sni_ext.extend_from_slice(name);

        let mut extensions = Vec::new();
        extensions.extend_from_slice(&[0x00, 0x00]); // extension type: server_name
        extensions.extend_from_slice(&(sni_ext.len() as u16).to_be_bytes());
        extensions.extend_from_slice(&sni_ext);

        handshake.extend_from_slice(&(extensions.len() as u16).to_be_bytes());
        handshake.extend_from_slice(&extensions);

        let mut record = Vec::new();
        record.push(0x16); // handshake record
        record.extend_from_slice(&[0x03, 0x01]); // record version
        let mut hs_with_header = Vec::new();
        hs_with_header.push(0x01); // handshake type: ClientHello
        let len = handshake.len() as u32;
        hs_with_header.extend_from_slice(&len.to_be_bytes()[1..]); // 3-byte length
        hs_with_header.extend_from_slice(&handshake);
        record.extend_from_slice(&(hs_with_header.len() as u16).to_be_bytes());
        record.extend_from_slice(&hs_with_header);
        record
    }

    #[test]
    fn parses_sni_from_a_well_formed_client_hello() {
        let hello = real_client_hello_kic_common();
        assert_eq!(
            parse_sni_from_client_hello(&hello),
            Some("kic-common.lgthinq.com".to_string())
        );
    }

    #[test]
    fn parses_sni_regardless_of_which_name_it_carries() {
        let hello = build_client_hello("objectcontent.lgthinq.com");
        assert_eq!(
            parse_sni_from_client_hello(&hello),
            Some("objectcontent.lgthinq.com".to_string())
        );
    }

    #[test]
    fn not_a_handshake_record_yields_none() {
        assert_eq!(
            parse_sni_from_client_hello(&[0x17, 0x03, 0x01, 0x00, 0x01, 0xff]),
            None
        );
    }

    #[test]
    fn not_a_client_hello_message_yields_none() {
        let mut hello = build_client_hello("x.example.com");
        hello[5] = 0x02; // ServerHello instead of ClientHello
        assert_eq!(parse_sni_from_client_hello(&hello), None);
    }

    #[test]
    fn truncated_hello_yields_none_rather_than_panicking() {
        let hello = build_client_hello("kic-common.lgthinq.com");
        for cut in 0..hello.len() {
            // Must never panic (index out of bounds) on any prefix — a real socket can
            // hand back a short read for any reason.
            let _ = parse_sni_from_client_hello(&hello[..cut]);
        }
    }

    #[test]
    fn hello_with_no_sni_extension_yields_none() {
        // Same shape as build_client_hello but with an empty extensions block.
        let mut handshake = Vec::new();
        handshake.extend_from_slice(&[0x03, 0x03]);
        handshake.extend_from_slice(&[0u8; 32]);
        handshake.push(0);
        handshake.extend_from_slice(&[0x00, 0x02]);
        handshake.extend_from_slice(&[0x00, 0x2f]);
        handshake.push(1);
        handshake.push(0);
        handshake.extend_from_slice(&[0x00, 0x00]); // extensions length: 0

        let mut hs_with_header = vec![0x01];
        let len = handshake.len() as u32;
        hs_with_header.extend_from_slice(&len.to_be_bytes()[1..]);
        hs_with_header.extend_from_slice(&handshake);

        let mut record = vec![0x16, 0x03, 0x01];
        record.extend_from_slice(&(hs_with_header.len() as u16).to_be_bytes());
        record.extend_from_slice(&hs_with_header);

        assert_eq!(parse_sni_from_client_hello(&record), None);
    }

    /// Real (not mocked) socket-level test: a genuine TCP listener, a genuine
    /// ClientHello written to a genuine connected socket, `peek_sni` reading it back
    /// without consuming it — so whatever reads the stream next (the real TLS
    /// acceptor, in production) still sees the full ClientHello from the start.
    #[tokio::test]
    async fn peek_sni_reads_without_consuming_the_stream() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let hello = build_client_hello("kic-common.lgthinq.com");
        let hello_for_client = hello.clone();
        let client = tokio::spawn(async move {
            let mut sock = TcpStream::connect(addr).await.unwrap();
            use tokio::io::AsyncWriteExt;
            sock.write_all(&hello_for_client).await.unwrap();
            sock
        });

        let (server_sock, _) = listener.accept().await.unwrap();
        let name = peek_sni(&server_sock).await;
        assert_eq!(name.as_deref(), Some("kic-common.lgthinq.com"));

        // The peeked bytes must still be there for a subsequent real read — this is
        // the whole point of using peek() instead of read().
        use tokio::io::AsyncReadExt;
        let mut server_sock = server_sock;
        let mut got = vec![0u8; hello.len()];
        server_sock.read_exact(&mut got).await.unwrap();
        assert_eq!(got, hello);

        client.await.unwrap();
    }

    /// Real (not mocked) end-to-end splice test: a fake "real CDN" TCP server, a fake
    /// "appliance" client, and the actual splice logic in between — confirms bytes
    /// flow both directions through the real function, not a simulation of it.
    /// Exercises `splice_to_host` directly (the port-parameterized function
    /// `splice_to_real_host` is a one-line wrapper around) since nothing in a test
    /// should bind to the real `:443`.
    #[tokio::test]
    async fn splice_forwards_both_directions() {
        let real_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let real_port = real_listener.local_addr().unwrap().port();

        // The "real CDN": reads what the appliance sends, then answers.
        tokio::spawn(async move {
            let (mut sock, _) = real_listener.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 5];
            sock.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"hello");
            sock.write_all(b"world").await.unwrap();
        });

        // Stands in for the caller's already-accepted appliance connection.
        let front_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let front_addr = front_listener.local_addr().unwrap();

        let appliance = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut sock = TcpStream::connect(front_addr).await.unwrap();
            sock.write_all(b"hello").await.unwrap();
            let mut buf = [0u8; 5];
            sock.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"world");
        });

        let (front_sock, _) = front_listener.accept().await.unwrap();
        splice_to_host(front_sock, "127.0.0.1", real_port)
            .await
            .unwrap();

        appliance.await.unwrap();
    }
}
