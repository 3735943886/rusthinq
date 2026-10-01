//! Custom DNS / DNS-over-HTTPS resolution for the bridge's own outbound
//! connections to the real LG cloud (port of `bridge/resolver.ts`).
//!
//! The appliances find rusthinq through a DNS-level redirect of the ThinQ
//! hostnames, and the host running rusthinq usually sits behind the same
//! resolver. Left to the system resolver, the bridge's own upstream
//! connections (`oauth2.rs`, `thinq_api.rs`, `pair.rs`'s HTTP calls,
//! `thinq1_conn.rs`'s raw TCP+TLS, `thinq2_conn.rs`'s MQTT connection) would
//! resolve straight back to rusthinq instead of the real LG cloud. With
//! [`set_servers`] given a non-empty list, those connections resolve through
//! DNS-over-HTTPS (RFC 8484) or plain DNS servers of their own instead. With
//! none configured (the default), everything here defers to the system
//! resolver and behaves exactly like a plain `reqwest::Client::new()`/
//! `TcpStream::connect` would.

use anyhow::{Context, Result, anyhow, bail};
use rusthinq_util::sync::Mutex;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;

const TIMEOUT: Duration = Duration::from_secs(5);
const MIN_TTL: u64 = 60;
const MAX_TTL: u64 = 3600;
const TYPE_A: u16 = 1;
const TYPE_AAAA: u16 = 28;

#[derive(Debug, Clone, PartialEq)]
struct Record {
    addr: IpAddr,
    ttl: u32,
}

#[derive(Debug, Clone)]
enum Server {
    Doh(url::Url),
    Plain(SocketAddr),
}

impl Server {
    fn name(&self) -> String {
        match self {
            Server::Doh(u) => u.to_string(),
            Server::Plain(a) => a.to_string(),
        }
    }
}

/// Each entry is a DoH URL (`https://.../dns-query`) or a plain DNS server
/// address, optionally with a port (default 53). Rejects anything else --
/// same as upstream's `setServers` throwing on a garbage entry.
fn parse_server(entry: &str) -> Result<Server> {
    if entry.starts_with("https://") {
        let url = url::Url::parse(entry).with_context(|| format!("invalid DoH URL {entry}"))?;
        return Ok(Server::Doh(url));
    }
    if let Ok(addr) = entry.parse::<SocketAddr>() {
        return Ok(Server::Plain(addr));
    }
    if let Ok(ip) = entry.parse::<IpAddr>() {
        return Ok(Server::Plain(SocketAddr::new(ip, 53)));
    }
    bail!("not a DNS-over-HTTPS URL or a plain DNS server address: {entry}")
}

static SERVERS: Mutex<Vec<Server>> = Mutex::new(Vec::new());
static CACHE: LazyLock<Mutex<HashMap<String, CacheEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static PENDING: LazyLock<Mutex<HashMap<String, Arc<OnceCell<ResolveResult>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

struct CacheEntry {
    addrs: Vec<IpAddr>,
    expires: Instant,
}

/// `Err` carries a plain message rather than `anyhow::Error` so the result can
/// be cached and cloned to every caller `resolve_host` coalesced together.
type ResolveResult = Result<Vec<IpAddr>, String>;

/// Replace the configured resolvers, trying each in order until one answers.
/// An empty list (the default) means "use the system resolver". Rejects any
/// entry that isn't a DoH URL or a plain DNS server address.
pub fn set_servers(entries: &[String]) -> Result<()> {
    let servers = entries
        .iter()
        .map(|e| parse_server(e))
        .collect::<Result<Vec<_>>>()?;
    *SERVERS.lock() = servers;
    CACHE.lock().clear();
    Ok(())
}

fn has_servers() -> bool {
    !SERVERS.lock().is_empty()
}

fn cache_get(hostname: &str) -> Option<Vec<IpAddr>> {
    let cache = CACHE.lock();
    cache
        .get(hostname)
        .filter(|e| e.expires > Instant::now())
        .map(|e| e.addrs.clone())
}

fn cache_put(hostname: &str, addrs: Vec<IpAddr>, ttl: u64) {
    CACHE.lock().insert(
        hostname.to_string(),
        CacheEntry {
            addrs,
            expires: Instant::now() + Duration::from_secs(ttl),
        },
    );
}

/// Resolve `hostname` to a list of addresses, via the configured servers (or
/// the system resolver if none are set). An IP literal is returned as-is
/// without ever consulting a server. Concurrent calls for the same hostname
/// (e.g. HTTP + MQTT connecting together at bridge startup) share one query.
pub async fn resolve_host(hostname: &str) -> Result<Vec<IpAddr>> {
    if let Ok(ip) = hostname.parse::<IpAddr>() {
        return Ok(vec![ip]);
    }
    if !has_servers() {
        return system_lookup(hostname).await;
    }
    if let Some(addrs) = cache_get(hostname) {
        return Ok(addrs);
    }

    let cell = {
        let mut pending = PENDING.lock();
        pending
            .entry(hostname.to_string())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone()
    };
    let result = cell
        .get_or_init(|| query_servers(hostname.to_string()))
        .await
        .clone();
    PENDING.lock().remove(hostname);
    result.map_err(|e| anyhow!(e))
}

async fn system_lookup(hostname: &str) -> Result<Vec<IpAddr>> {
    let addrs = tokio::net::lookup_host((hostname, 0))
        .await
        .with_context(|| format!("resolve {hostname}"))?
        .map(|a| a.ip())
        .collect();
    Ok(addrs)
}

async fn query_servers(hostname: String) -> ResolveResult {
    let servers = SERVERS.lock().clone();
    let mut last_err: Option<String> = None;
    for server in &servers {
        match query_server(server, &hostname).await {
            Ok(records) if !records.is_empty() => {
                let ttl = records
                    .iter()
                    .map(|r| r.ttl as u64)
                    .min()
                    .unwrap_or(MIN_TTL)
                    .clamp(MIN_TTL, MAX_TTL);
                let addrs: Vec<IpAddr> = records.into_iter().map(|r| r.addr).collect();
                cache_put(&hostname, addrs.clone(), ttl);
                tracing::debug!(
                    "resolved {hostname} via {}: {}",
                    server.name(),
                    addrs
                        .iter()
                        .map(|a| a.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                return Ok(addrs);
            }
            // An empty-but-successful answer (NXDOMAIN) is authoritative --
            // don't keep trying other servers for a name that doesn't exist.
            Ok(_) => break,
            Err(e) => {
                tracing::debug!("resolving {hostname} via {} failed: {e}", server.name());
                last_err = Some(e.to_string());
            }
        }
    }
    Err(match last_err {
        Some(e) => format!("can't resolve {hostname}: {e}"),
        None => format!("can't resolve {hostname}"),
    })
}

async fn query_server(server: &Server, hostname: &str) -> Result<Vec<Record>> {
    // IPv4 first: an IPv6-only answer is of no use to a host without IPv6
    // connectivity, and callers mostly want a single address.
    let (v4, v6) = tokio::join!(
        query_type(server, hostname, TYPE_A),
        query_type(server, hostname, TYPE_AAAA),
    );
    match (v4, v6) {
        (Err(e), Err(_)) => Err(e),
        (r4, r6) => {
            let mut out = r4.unwrap_or_default();
            out.extend(r6.unwrap_or_default());
            Ok(out)
        }
    }
}

async fn query_type(server: &Server, hostname: &str, qtype: u16) -> Result<Vec<Record>> {
    match server {
        Server::Doh(url) => doh_query(url, hostname, qtype).await,
        Server::Plain(addr) => plain_query(*addr, hostname, qtype).await,
    }
}

#[allow(
    clippy::expect_used,
    reason = "the client only fails to build if the TLS backend cannot initialise"
)]
fn doh_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    // Deliberately the plain system-resolver client, never `http_client()` --
    // a DoH server given by hostname would otherwise need itself resolved
    // first. DoH-by-IP-address (the common case, e.g. 1.1.1.1) sidesteps this
    // entirely, per the config doc comment.
    CLIENT.get_or_init(|| reqwest::Client::builder().build().expect("reqwest client"))
}

async fn doh_query(url: &url::Url, hostname: &str, qtype: u16) -> Result<Vec<Record>> {
    let query = encode_query(hostname, qtype)?;
    let encoded = base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, &query);
    let mut target = url.clone();
    target.query_pairs_mut().append_pair("dns", &encoded);
    let resp = doh_client()
        .get(target)
        .header("accept", "application/dns-message")
        .timeout(TIMEOUT)
        .send()
        .await
        .with_context(|| format!("DoH request to {url}"))?;
    if !resp.status().is_success() {
        bail!("HTTP {}", resp.status());
    }
    let body = resp
        .bytes()
        .await
        .with_context(|| format!("DoH response body from {url}"))?;
    parse_response(&body, qtype)
}

async fn plain_query(addr: SocketAddr, hostname: &str, qtype: u16) -> Result<Vec<Record>> {
    let query = encode_query(hostname, qtype)?;
    let bind_addr = if addr.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = tokio::net::UdpSocket::bind(bind_addr)
        .await
        .context("bind udp socket")?;
    socket
        .connect(addr)
        .await
        .with_context(|| format!("connect to {addr}"))?;

    let mut last_err = None;
    for _ in 0..2 {
        if let Err(e) = socket.send(&query).await {
            last_err = Some(anyhow!(e));
            continue;
        }
        let mut buf = [0u8; 4096];
        match tokio::time::timeout(TIMEOUT, socket.recv(&mut buf)).await {
            Ok(Ok(n)) => return parse_response(buf.get(..n).unwrap_or_default(), qtype),
            Ok(Err(e)) => last_err = Some(anyhow!(e)),
            Err(_) => last_err = Some(anyhow!("timeout querying {addr}")),
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("no response from {addr}")))
}

/// Encode a DNS query for `hostname`/`qtype` in wire format (RFC 1035 §4.1),
/// recursion desired, ID left at 0 as RFC 8484 recommends for DoH.
fn encode_query(hostname: &str, qtype: u16) -> Result<Vec<u8>> {
    // ID 0, flags 0x0100 (RD), QDCOUNT 1, no other records.
    let mut buf = vec![0, 0, 0x01, 0x00, 0, 0x01, 0, 0, 0, 0, 0, 0];

    let trimmed = hostname.strip_suffix('.').unwrap_or(hostname);
    for label in trimmed.split('.') {
        let bytes = label.as_bytes();
        if bytes.is_empty() || bytes.len() > 63 || !bytes.is_ascii() {
            bail!("invalid hostname {hostname}");
        }
        buf.push(bytes.len() as u8);
        buf.extend_from_slice(bytes);
    }
    buf.push(0); // root label
    buf.extend_from_slice(&qtype.to_be_bytes());
    buf.extend_from_slice(&1u16.to_be_bytes()); // class IN
    Ok(buf)
}

/// Parse a DNS response, returning the addresses of the requested type.
/// CNAME records the server followed on the way come along in the answer
/// section and are skipped (any `rtype` other than `qtype` is).
fn parse_response(msg: &[u8], qtype: u16) -> Result<Vec<Record>> {
    let truncated = || anyhow!("truncated DNS message");
    let &[_, _, _, flags_lo, qd_hi, qd_lo, an_hi, an_lo, ..] = msg else {
        return Err(truncated());
    };
    if msg.len() < 12 {
        return Err(truncated());
    }
    let rcode = flags_lo & 0x0f;
    if rcode == 3 {
        return Ok(Vec::new()); // NXDOMAIN
    }
    if rcode != 0 {
        bail!("DNS error code {rcode}");
    }

    let questions = u16::from_be_bytes([qd_hi, qd_lo]) as usize;
    let answers = u16::from_be_bytes([an_hi, an_lo]) as usize;

    let mut offset = 12;
    for _ in 0..questions {
        offset = skip_name(msg, offset)?.saturating_add(4);
    }

    let mut records = Vec::new();
    for _ in 0..answers {
        offset = skip_name(msg, offset)?;
        let header: &[u8; 10] = msg
            .get(offset..offset.saturating_add(10))
            .and_then(|s| s.try_into().ok())
            .ok_or_else(truncated)?;
        let &[t_hi, t_lo, _, _, ttl0, ttl1, ttl2, ttl3, len_hi, len_lo] = header;
        let rtype = u16::from_be_bytes([t_hi, t_lo]);
        let ttl = u32::from_be_bytes([ttl0, ttl1, ttl2, ttl3]);
        let length = u16::from_be_bytes([len_hi, len_lo]) as usize;
        let data_start = offset + 10;
        let data_end = data_start.saturating_add(length);
        let data = msg.get(data_start..data_end).ok_or_else(truncated)?;
        offset = data_end;

        if rtype != qtype {
            continue;
        }
        if qtype == TYPE_A
            && let &[a, b, c, d] = data
        {
            records.push(Record {
                addr: IpAddr::V4(Ipv4Addr::new(a, b, c, d)),
                ttl,
            });
        }
        if qtype == TYPE_AAAA
            && let Ok(octets) = <[u8; 16]>::try_from(data)
        {
            records.push(Record {
                addr: IpAddr::V6(Ipv6Addr::from(octets)),
                ttl,
            });
        }
    }
    Ok(records)
}

/// A DNS name is either a sequence of length-prefixed labels ending in a
/// zero-length root label, or ends early with a compression pointer (RFC
/// 1035 §4.1.4) -- either way this only needs to know where the name *ends*,
/// never what it says, so a pointer is never followed.
fn skip_name(msg: &[u8], mut offset: usize) -> Result<usize> {
    loop {
        let length = *msg
            .get(offset)
            .ok_or_else(|| anyhow!("truncated DNS message"))?;
        if length == 0 {
            return Ok(offset + 1);
        }
        if length & 0xc0 == 0xc0 {
            return Ok(offset + 2);
        }
        offset += length as usize + 1;
    }
}

/// A `reqwest::Client` sharing this module's resolver -- a drop-in for
/// `reqwest::Client::new()` that behaves identically when no servers are
/// configured (falls through to [`system_lookup`]).
#[allow(
    clippy::expect_used,
    reason = "the client only fails to build if the TLS backend cannot initialise"
)]
pub fn http_client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .dns_resolver(Arc::new(DnsResolver))
                .build()
                .expect("reqwest client")
        })
        .clone()
}

/// A `reqwest::dns::Resolve` sharing this module's resolver, for callers that
/// need to customize other `ClientBuilder` options too (e.g.
/// `thinq1_conn.rs`'s per-connection timeout) rather than reuse
/// [`http_client`]'s shared client outright.
pub fn dns_resolver() -> Arc<dyn reqwest::dns::Resolve> {
    Arc::new(DnsResolver)
}

/// Resolve `host` and connect to the first address that accepts a TCP
/// connection -- the same resolution [`install_socket_connector`] gives
/// rumqttc, for callers dialing their own raw `TcpStream` instead (e.g.
/// `thinq1_conn.rs`'s ThinQ1 RTI connection).
pub async fn connect_tcp(host: &str, port: u16) -> std::io::Result<tokio::net::TcpStream> {
    let addrs = resolve_host(host)
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let mut last_err = None;
    for ip in addrs {
        match tokio::net::TcpStream::connect((ip, port)).await {
            Ok(stream) => return Ok(stream),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("no address for {host}"),
        )
    }))
}

struct DnsResolver;

impl reqwest::dns::Resolve for DnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addrs = resolve_host(&host).await.map_err(
                |e| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() },
            )?;
            let iter: reqwest::dns::Addrs =
                Box::new(addrs.into_iter().map(|ip| SocketAddr::new(ip, 0)));
            Ok(iter)
        })
    }
}

/// Install a `set_socket_connector` on `opts` that resolves the broker
/// hostname through this module's resolver before dialing it, while leaving
/// `opts`'s own hostname (used for the TLS `ServerName`/SNI on the connection
/// rumqttc layers on top) untouched -- see `thinq2_conn.rs`'s call site for
/// why this is the whole point of #50.
pub fn install_socket_connector(opts: &mut rumqttc::MqttOptions) {
    opts.set_socket_connector(|host, _network_options| async move {
        let (hostname, port) = host.rsplit_once(':').ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("bad host {host}"))
        })?;
        let port: u16 = port.parse().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("bad port in {host}"),
            )
        })?;
        connect_tcp(hostname, port).await
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        rusthinq_util::hex::decode(s).unwrap()
    }

    /// Builds a response to `query` with a CNAME followed by the given
    /// records, using compression pointers the way real servers do --
    /// mirrors `tests/bridge/resolver.test.ts`'s own `response()` helper.
    fn build_response(query: &[u8], answers: &[(u16, u32, &[u8])], rcode: u16) -> Vec<u8> {
        let mut header = query[0..12].to_vec();
        let flags: u16 = 0x8180 | rcode;
        header[2] = (flags >> 8) as u8;
        header[3] = (flags & 0xff) as u8;
        let ancount = (answers.len() as u16) + 1;
        header[6] = (ancount >> 8) as u8;
        header[7] = (ancount & 0xff) as u8;

        let cname_target = hex("0463646e73c00c"); // "cdns" + pointer to the question name
        let rr = |rtype: u16, ttl: u32, data: &[u8], name: &[u8]| -> Vec<u8> {
            let mut out = name.to_vec();
            out.extend_from_slice(&rtype.to_be_bytes());
            out.extend_from_slice(&1u16.to_be_bytes());
            out.extend_from_slice(&ttl.to_be_bytes());
            out.extend_from_slice(&(data.len() as u16).to_be_bytes());
            out.extend_from_slice(data);
            out
        };

        let question_end = query.len();
        let cname_rdata_offset = question_end + 2 + 10;
        let pointer_to_cname = [
            0xc0 | ((cname_rdata_offset >> 8) as u8),
            (cname_rdata_offset & 0xff) as u8,
        ];

        let mut out = header;
        out.extend_from_slice(&query[12..]);
        out.extend(rr(5, 300, &cname_target, &[0xc0, 0x0c]));
        for (t, ttl, data) in answers {
            out.extend(rr(*t, *ttl, data, &pointer_to_cname));
        }
        out
    }

    #[test]
    fn encode_query_matches_the_wire_format_vector() {
        let query = encode_query("common.lgthinq.com", TYPE_A).unwrap();
        assert_eq!(u16::from_be_bytes([query[0], query[1]]), 0); // ID
        assert_eq!(u16::from_be_bytes([query[2], query[3]]), 0x0100); // RD
        assert_eq!(u16::from_be_bytes([query[4], query[5]]), 1); // QDCOUNT
        assert_eq!(
            &query[12..],
            hex("06636f6d6d6f6e076c677468696e7103636f6d0000010001").as_slice()
        );
        assert!(encode_query("bad..name", TYPE_A).is_err());
    }

    #[test]
    fn parse_response_follows_compression_and_skips_cnames() {
        let a = encode_query("common.lgthinq.com", TYPE_A).unwrap();
        let resp = build_response(
            &a,
            &[(TYPE_A, 120, &[52, 1, 2, 3]), (TYPE_A, 90, &[52, 1, 2, 4])],
            0,
        );
        let records = parse_response(&resp, TYPE_A).unwrap();
        assert_eq!(
            records,
            vec![
                Record {
                    addr: IpAddr::V4(Ipv4Addr::new(52, 1, 2, 3)),
                    ttl: 120
                },
                Record {
                    addr: IpAddr::V4(Ipv4Addr::new(52, 1, 2, 4)),
                    ttl: 90
                },
            ]
        );

        let aaaa = encode_query("common.lgthinq.com", TYPE_AAAA).unwrap();
        let data = hex("20010db8000000000000000000000001");
        let resp2 = build_response(&aaaa, &[(TYPE_AAAA, 60, &data)], 0);
        assert_eq!(
            parse_response(&resp2, TYPE_AAAA).unwrap(),
            vec![Record {
                addr: "2001:db8::1".parse().unwrap(),
                ttl: 60
            }]
        );
    }

    #[test]
    fn parse_response_handles_nxdomain_and_errors() {
        let q = encode_query("nope.example", TYPE_A).unwrap();
        assert_eq!(
            parse_response(&build_response(&q, &[], 3), TYPE_A).unwrap(),
            Vec::new()
        );
        assert!(parse_response(&build_response(&q, &[], 2), TYPE_A).is_err());
    }

    #[test]
    fn parse_response_never_panics_on_truncated_or_corrupted_input() {
        let a = encode_query("common.lgthinq.com", TYPE_A).unwrap();
        let v4 = build_response(&a, &[(TYPE_A, 120, &[52, 1, 2, 3])], 0);
        let aaaa = encode_query("common.lgthinq.com", TYPE_AAAA).unwrap();
        let v6 = build_response(
            &aaaa,
            &[(TYPE_AAAA, 60, &hex("20010db8000000000000000000000001"))],
            0,
        );
        for (msg, qtype) in [(&v4, TYPE_A), (&v6, TYPE_AAAA)] {
            for cut in 0..=msg.len() {
                let _ = parse_response(&msg[..cut], qtype);
            }
            for i in 0..msg.len() {
                for v in [0x00u8, 0x01, 0x3f, 0x7f, 0xc0, 0xff] {
                    let mut m = msg.clone();
                    m[i] = v;
                    let _ = parse_response(&m, qtype);
                    let _ = parse_response(&m[..i], qtype);
                }
            }
        }
    }

    #[test]
    fn parse_server_accepts_doh_urls_and_plain_addresses_rejects_garbage() {
        assert!(matches!(
            parse_server("https://1.1.1.1/dns-query").unwrap(),
            Server::Doh(_)
        ));
        match parse_server("1.1.1.1").unwrap() {
            Server::Plain(a) => assert_eq!(a.port(), 53),
            other => panic!("expected Server::Plain, got {other:?}"),
        }
        match parse_server("1.1.1.1:5353").unwrap() {
            Server::Plain(a) => assert_eq!(a.port(), 5353),
            other => panic!("expected Server::Plain, got {other:?}"),
        }
        assert!(parse_server("not a server").is_err());
        assert!(parse_server("system").is_err());
    }

    // These tests share this module's global resolver state (`SERVERS`/`CACHE`), so
    // they're combined into one sequential test rather than left to run in
    // parallel against each other under cargo's default test runner.
    #[tokio::test]
    async fn resolve_host_end_to_end() {
        set_servers(&[]).unwrap();

        // No servers configured -> falls back to the system resolver.
        let addrs = resolve_host("localhost").await.unwrap();
        assert!(!addrs.is_empty());

        // An IP literal never touches the configured servers, even an
        // unreachable one.
        set_servers(&["https://192.0.2.1/dns-query".to_string()]).unwrap();
        assert_eq!(
            resolve_host("127.0.0.1").await.unwrap(),
            vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))]
        );
        assert_eq!(
            resolve_host("::1").await.unwrap(),
            vec![IpAddr::V6(Ipv6Addr::LOCALHOST)]
        );

        // A real plain-DNS round trip against a local fake server.
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = socket.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = [0u8; 512];
            while let Ok((n, peer)) = socket.recv_from(&mut buf).await {
                let query = &buf[..n];
                let qtype = u16::from_be_bytes([query[query.len() - 4], query[query.len() - 3]]);
                let resp = if qtype == TYPE_A {
                    build_response(query, &[(TYPE_A, 120, &[10, 0, 0, 1])], 0)
                } else {
                    build_response(query, &[], 0)
                };
                let _ = socket.send_to(&resp, peer).await;
            }
        });
        set_servers(&[server_addr.to_string()]).unwrap();
        assert_eq!(
            resolve_host("fake.example.test").await.unwrap(),
            vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))]
        );
        // Served from cache on a second call -- confirmed indirectly by the
        // TTL-backed cache entry now existing.
        assert!(cache_get("fake.example.test").is_some());

        set_servers(&[]).unwrap();
        assert!(set_servers(&["not a server".to_string()]).is_err());
        assert!(set_servers(&["system".to_string()]).is_err());
        set_servers(&[]).unwrap();
    }
}
