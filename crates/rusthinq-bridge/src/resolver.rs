//! Outbound name resolution for connections to the real LG cloud (0.1 `[bridge] dns`).
//!
//! Appliances usually find rusthinq through a DNS-level redirect of the ThinQ names, and
//! the host running rusthinq often sits behind the same resolver. Left to the system
//! resolver, the bridge's own upstream connections would resolve back to rusthinq.
//! [`set_servers`] makes them resolve through DNS-over-HTTPS (RFC 8484) or plain DNS
//! servers instead, tried in order. With none configured (the default) every lookup is
//! the system resolver, exactly as without this module.
use std::{
    collections::HashMap,
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{Arc, Mutex, OnceLock, RwLock},
    time::{Duration, Instant},
};
use tokio::{net::TcpStream, sync::OnceCell};

const TIMEOUT: Duration = Duration::from_secs(5);
const MIN_TTL: u64 = 60;
const MAX_TTL: u64 = 3600;
const MAX_SERVERS: usize = 8;
const MAX_CACHE: usize = 256;
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

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// A DoH URL (`https://.../dns-query`) or a plain DNS server address with an optional
/// port (default 53). Anything else is rejected.
fn parse_server(entry: &str) -> io::Result<Server> {
    if entry.starts_with("https://") {
        let url =
            url::Url::parse(entry).map_err(|_| invalid(format!("invalid DoH URL {entry}")))?;
        return Ok(Server::Doh(url));
    }
    if let Ok(address) = entry.parse::<SocketAddr>() {
        return Ok(Server::Plain(address));
    }
    if let Ok(ip) = entry.parse::<IpAddr>() {
        return Ok(Server::Plain(SocketAddr::new(ip, 53)));
    }
    Err(invalid(format!(
        "not a DNS-over-HTTPS URL or a plain DNS server address: {entry}"
    )))
}

struct CacheEntry {
    addrs: Vec<IpAddr>,
    expires: Instant,
}

/// Errors are kept as messages so one coalesced result can be cloned to every caller.
type Resolved = Result<Vec<IpAddr>, String>;

#[derive(Default)]
struct Resolver {
    servers: Vec<Server>,
    cache: Mutex<HashMap<String, CacheEntry>>,
    pending: Mutex<HashMap<String, Arc<OnceCell<Resolved>>>>,
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn global() -> &'static RwLock<Arc<Resolver>> {
    static GLOBAL: OnceLock<RwLock<Arc<Resolver>>> = OnceLock::new();
    GLOBAL.get_or_init(Default::default)
}

fn current() -> Arc<Resolver> {
    global().read().unwrap_or_else(|e| e.into_inner()).clone()
}

/// Replace the configured resolvers. An empty list means the system resolver.
pub fn set_servers(entries: &[String]) -> io::Result<()> {
    *global().write().unwrap_or_else(|e| e.into_inner()) = Arc::new(Resolver::new(entries)?);
    Ok(())
}

impl Resolver {
    fn new(entries: &[String]) -> io::Result<Self> {
        if entries.len() > MAX_SERVERS {
            return Err(invalid("too many DNS servers".into()));
        }
        Ok(Self {
            servers: entries
                .iter()
                .map(|entry| parse_server(entry))
                .collect::<io::Result<_>>()?,
            ..Default::default()
        })
    }

    /// IP literals never consult a server. Concurrent lookups of one name share a query.
    async fn resolve(&self, hostname: &str) -> io::Result<Vec<IpAddr>> {
        if let Ok(ip) = hostname.parse::<IpAddr>() {
            return Ok(vec![ip]);
        }
        if self.servers.is_empty() {
            return Ok(tokio::net::lookup_host((hostname, 0))
                .await?
                .map(|a| a.ip())
                .collect());
        }
        if let Some(entry) = lock(&self.cache).get(hostname)
            && entry.expires > Instant::now()
        {
            return Ok(entry.addrs.clone());
        }
        let cell = lock(&self.pending)
            .entry(hostname.to_owned())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();
        let result = cell.get_or_init(|| self.query(hostname)).await.clone();
        lock(&self.pending).remove(hostname);
        result.map_err(io::Error::other)
    }

    async fn query(&self, hostname: &str) -> Resolved {
        let mut last = None;
        for server in &self.servers {
            match query_server(server, hostname).await {
                Ok(records) if !records.is_empty() => {
                    let ttl = records
                        .iter()
                        .map(|r| u64::from(r.ttl))
                        .min()
                        .unwrap_or(MIN_TTL)
                        .clamp(MIN_TTL, MAX_TTL);
                    let addrs: Vec<IpAddr> = records.into_iter().map(|r| r.addr).collect();
                    let mut cache = lock(&self.cache);
                    let now = Instant::now();
                    cache.retain(|_, entry| entry.expires > now);
                    if cache.len() < MAX_CACHE {
                        cache.insert(
                            hostname.to_owned(),
                            CacheEntry {
                                addrs: addrs.clone(),
                                expires: now + Duration::from_secs(ttl),
                            },
                        );
                    }
                    return Ok(addrs);
                }
                // An empty successful answer (NXDOMAIN) is authoritative.
                Ok(_) => break,
                Err(error) => last = Some(error.to_string()),
            }
        }
        Err(match last {
            Some(error) => format!("can't resolve {hostname}: {error}"),
            None => format!("can't resolve {hostname}"),
        })
    }
}

/// Resolve `hostname` through the configured servers, or the system resolver.
pub async fn resolve_host(hostname: &str) -> io::Result<Vec<IpAddr>> {
    current().resolve(hostname).await
}

/// Resolve `host` and connect to the first address that accepts TCP.
pub async fn connect_tcp(host: &str, port: u16) -> io::Result<TcpStream> {
    connect_with(&current(), host, port).await
}

async fn connect_with(resolver: &Resolver, host: &str, port: u16) -> io::Result<TcpStream> {
    let mut last = None;
    for ip in resolver.resolve(host).await? {
        match TcpStream::connect((ip, port)).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last = Some(error),
        }
    }
    Err(last.unwrap_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("no address for {host}"))
    }))
}

/// A `reqwest` resolver backed by this module, for every LG cloud HTTP client.
pub fn dns_resolver() -> Arc<impl reqwest::dns::Resolve + 'static> {
    Arc::new(DnsResolver)
}

struct DnsResolver;
impl reqwest::dns::Resolve for DnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let addrs = resolve_host(&host)
                .await
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
            let addrs: reqwest::dns::Addrs =
                Box::new(addrs.into_iter().map(|ip| SocketAddr::new(ip, 0)));
            Ok(addrs)
        })
    }
}

async fn query_server(server: &Server, hostname: &str) -> io::Result<Vec<Record>> {
    // IPv4 first: an IPv6-only answer is of no use to a host without IPv6.
    let (v4, v6) = tokio::join!(
        query_type(server, hostname, TYPE_A),
        query_type(server, hostname, TYPE_AAAA),
    );
    match (v4, v6) {
        (Err(error), Err(_)) => Err(error),
        (v4, v6) => {
            let mut records = v4.unwrap_or_default();
            records.extend(v6.unwrap_or_default());
            Ok(records)
        }
    }
}

async fn query_type(server: &Server, hostname: &str, qtype: u16) -> io::Result<Vec<Record>> {
    match server {
        Server::Doh(url) => doh_query(url, hostname, qtype).await,
        Server::Plain(address) => plain_query(*address, hostname, qtype).await,
    }
}

/// The DoH client itself uses the system resolver; a DoH server named by hostname would
/// otherwise need resolving through itself. DoH by IP address avoids the question.
fn doh_client() -> io::Result<&'static reqwest::Client> {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    if let Some(client) = CLIENT.get() {
        return Ok(client);
    }
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(io::Error::other)?;
    Ok(CLIENT.get_or_init(|| client))
}

async fn doh_query(url: &url::Url, hostname: &str, qtype: u16) -> io::Result<Vec<Record>> {
    use base64::Engine;
    let query = encode_query(hostname, qtype)?;
    let mut target = url.clone();
    target.query_pairs_mut().append_pair(
        "dns",
        &base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(query),
    );
    let response = doh_client()?
        .get(target)
        .header("accept", "application/dns-message")
        .timeout(TIMEOUT)
        .send()
        .await
        .map_err(|_| io::Error::other(format!("DoH request to {url} failed")))?;
    if !response.status().is_success() {
        return Err(io::Error::other(format!("DoH HTTP {}", response.status())));
    }
    if response.content_length().is_some_and(|n| n > 65535) {
        return Err(io::Error::other("DoH response exceeded"));
    }
    let body = response
        .bytes()
        .await
        .map_err(|_| io::Error::other(format!("DoH response from {url} failed")))?;
    if body.len() > 65535 {
        return Err(io::Error::other("DoH response exceeded"));
    }
    parse_response(&body, qtype)
}

async fn plain_query(address: SocketAddr, hostname: &str, qtype: u16) -> io::Result<Vec<Record>> {
    let query = encode_query(hostname, qtype)?;
    let local = if address.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = tokio::net::UdpSocket::bind(local).await?;
    socket.connect(address).await?;
    let mut last = None;
    for _ in 0..2 {
        if let Err(error) = socket.send(&query).await {
            last = Some(error);
            continue;
        }
        let mut buffer = [0u8; 4096];
        match tokio::time::timeout(TIMEOUT, socket.recv(&mut buffer)).await {
            Ok(Ok(n)) => return parse_response(buffer.get(..n).unwrap_or_default(), qtype),
            Ok(Err(error)) => last = Some(error),
            Err(_) => {
                last = Some(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("timeout querying {address}"),
                ))
            }
        }
    }
    Err(last.unwrap_or_else(|| io::Error::other(format!("no response from {address}"))))
}

/// RFC 1035 §4.1 query, recursion desired, ID 0 as RFC 8484 recommends.
fn encode_query(hostname: &str, qtype: u16) -> io::Result<Vec<u8>> {
    let mut buffer = vec![0, 0, 0x01, 0x00, 0, 0x01, 0, 0, 0, 0, 0, 0];
    let trimmed = hostname.strip_suffix('.').unwrap_or(hostname);
    for label in trimmed.split('.') {
        let bytes = label.as_bytes();
        if bytes.is_empty() || bytes.len() > 63 || !bytes.is_ascii() {
            return Err(invalid(format!("invalid hostname {hostname}")));
        }
        buffer.push(bytes.len() as u8);
        buffer.extend_from_slice(bytes);
    }
    buffer.push(0);
    buffer.extend_from_slice(&qtype.to_be_bytes());
    buffer.extend_from_slice(&1u16.to_be_bytes());
    Ok(buffer)
}

/// Addresses of the requested type. CNAMEs and other record types are skipped.
fn parse_response(message: &[u8], qtype: u16) -> io::Result<Vec<Record>> {
    let truncated = || io::Error::new(io::ErrorKind::InvalidData, "truncated DNS message");
    let &[_, _, _, flags, qd_hi, qd_lo, an_hi, an_lo, ..] = message else {
        return Err(truncated());
    };
    if message.len() < 12 {
        return Err(truncated());
    }
    match flags & 0x0f {
        3 => return Ok(Vec::new()),
        0 => {}
        code => return Err(io::Error::other(format!("DNS error code {code}"))),
    }
    let questions = usize::from(u16::from_be_bytes([qd_hi, qd_lo]));
    let answers = usize::from(u16::from_be_bytes([an_hi, an_lo]));
    let mut offset = 12;
    for _ in 0..questions {
        offset = skip_name(message, offset)?.saturating_add(4);
    }
    let mut records = Vec::new();
    for _ in 0..answers {
        offset = skip_name(message, offset)?;
        let header: &[u8; 10] = message
            .get(offset..offset.saturating_add(10))
            .and_then(|s| s.try_into().ok())
            .ok_or_else(truncated)?;
        let &[t_hi, t_lo, _, _, ttl0, ttl1, ttl2, ttl3, len_hi, len_lo] = header;
        let rtype = u16::from_be_bytes([t_hi, t_lo]);
        let ttl = u32::from_be_bytes([ttl0, ttl1, ttl2, ttl3]);
        let start = offset + 10;
        let end = start.saturating_add(usize::from(u16::from_be_bytes([len_hi, len_lo])));
        let data = message.get(start..end).ok_or_else(truncated)?;
        offset = end;
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

/// Where a name ends: a root label, or a compression pointer (never followed).
fn skip_name(message: &[u8], mut offset: usize) -> io::Result<usize> {
    loop {
        let length = *message
            .get(offset)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated DNS message"))?;
        if length == 0 {
            return Ok(offset + 1);
        }
        if length & 0xc0 == 0xc0 {
            return Ok(offset + 2);
        }
        offset += usize::from(length) + 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// A response to `query`: a CNAME, then `answers`, using compression pointers.
    fn build_response(query: &[u8], answers: &[(u16, u32, &[u8])], rcode: u16) -> Vec<u8> {
        let mut header = query[0..12].to_vec();
        let flags: u16 = 0x8180 | rcode;
        header[2..4].copy_from_slice(&flags.to_be_bytes());
        header[6..8].copy_from_slice(&(answers.len() as u16 + 1).to_be_bytes());
        let cname_target = hex("0463646e73c00c");
        let rr = |rtype: u16, ttl: u32, data: &[u8], name: &[u8]| -> Vec<u8> {
            let mut out = name.to_vec();
            out.extend_from_slice(&rtype.to_be_bytes());
            out.extend_from_slice(&1u16.to_be_bytes());
            out.extend_from_slice(&ttl.to_be_bytes());
            out.extend_from_slice(&(data.len() as u16).to_be_bytes());
            out.extend_from_slice(data);
            out
        };
        let cname_rdata = query.len() + 2 + 10;
        let pointer = [0xc0 | (cname_rdata >> 8) as u8, (cname_rdata & 0xff) as u8];
        let mut out = header;
        out.extend_from_slice(&query[12..]);
        out.extend(rr(5, 300, &cname_target, &[0xc0, 0x0c]));
        for (rtype, ttl, data) in answers {
            out.extend(rr(*rtype, *ttl, data, &pointer));
        }
        out
    }

    #[test]
    fn encode_query_matches_the_wire_format_vector() {
        let query = encode_query("common.lgthinq.com", TYPE_A).unwrap();
        assert_eq!(&query[..6], &[0, 0, 0x01, 0x00, 0, 1]);
        assert_eq!(
            &query[12..],
            hex("06636f6d6d6f6e076c677468696e7103636f6d0000010001").as_slice()
        );
        assert!(encode_query("bad..name", TYPE_A).is_err());
    }

    #[test]
    fn parse_response_follows_compression_skips_cnames_and_handles_nxdomain() {
        let a = encode_query("common.lgthinq.com", TYPE_A).unwrap();
        let response = build_response(
            &a,
            &[(TYPE_A, 120, &[52, 1, 2, 3]), (TYPE_A, 90, &[52, 1, 2, 4])],
            0,
        );
        assert_eq!(
            parse_response(&response, TYPE_A).unwrap(),
            vec![
                Record {
                    addr: "52.1.2.3".parse().unwrap(),
                    ttl: 120
                },
                Record {
                    addr: "52.1.2.4".parse().unwrap(),
                    ttl: 90
                },
            ]
        );
        let aaaa = encode_query("common.lgthinq.com", TYPE_AAAA).unwrap();
        let data = hex("20010db8000000000000000000000001");
        assert_eq!(
            parse_response(
                &build_response(&aaaa, &[(TYPE_AAAA, 60, &data)], 0),
                TYPE_AAAA
            )
            .unwrap(),
            vec![Record {
                addr: "2001:db8::1".parse().unwrap(),
                ttl: 60
            }]
        );
        assert!(
            parse_response(&build_response(&a, &[], 3), TYPE_A)
                .unwrap()
                .is_empty()
        );
        assert!(parse_response(&build_response(&a, &[], 2), TYPE_A).is_err());
    }

    #[test]
    fn parse_response_never_panics_on_truncated_or_corrupted_input() {
        let a = encode_query("common.lgthinq.com", TYPE_A).unwrap();
        let message = build_response(&a, &[(TYPE_A, 120, &[52, 1, 2, 3])], 0);
        for cut in 0..=message.len() {
            let _ = parse_response(&message[..cut], TYPE_A);
        }
        for i in 0..message.len() {
            for value in [0x00u8, 0x01, 0x3f, 0x7f, 0xc0, 0xff] {
                let mut corrupted = message.clone();
                corrupted[i] = value;
                let _ = parse_response(&corrupted, TYPE_A);
            }
        }
    }

    #[test]
    fn servers_accept_doh_urls_and_plain_addresses_and_reject_garbage() {
        assert!(matches!(
            parse_server("https://1.1.1.1/dns-query").unwrap(),
            Server::Doh(_)
        ));
        assert!(matches!(parse_server("1.1.1.1").unwrap(), Server::Plain(a) if a.port() == 53));
        assert!(
            matches!(parse_server("1.1.1.1:5353").unwrap(), Server::Plain(a) if a.port() == 5353)
        );
        assert!(parse_server("not a server").is_err());
        assert!(parse_server("system").is_err());
        assert!(Resolver::new(&vec!["1.1.1.1".into(); MAX_SERVERS + 1]).is_err());
    }

    #[tokio::test]
    async fn plain_dns_round_trip_caches_and_ip_literals_skip_servers() {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let queries = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = queries.clone();
        tokio::spawn(async move {
            let mut buffer = [0u8; 512];
            while let Ok((n, peer)) = socket.recv_from(&mut buffer).await {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let query = &buffer[..n];
                let qtype = u16::from_be_bytes([query[n - 4], query[n - 3]]);
                let response = if qtype == TYPE_A {
                    build_response(query, &[(TYPE_A, 120, &[127, 0, 0, 1])], 0)
                } else {
                    build_response(query, &[], 0)
                };
                let _ = socket.send_to(&response, peer).await;
            }
        });
        let resolver = Resolver::new(&[address.to_string()]).unwrap();
        let expected = vec![IpAddr::V4(Ipv4Addr::LOCALHOST)];
        assert_eq!(resolver.resolve("lg.example.test").await.unwrap(), expected);
        let after_first = queries.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(resolver.resolve("lg.example.test").await.unwrap(), expected);
        assert_eq!(
            queries.load(std::sync::atomic::Ordering::SeqCst),
            after_first
        );
        // The resolved address is what the bridge dials.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        connect_with(&resolver, "lg.example.test", port)
            .await
            .unwrap();
        let unreachable = Resolver::new(&["https://192.0.2.1/dns-query".into()]).unwrap();
        assert_eq!(
            unreachable.resolve("::1").await.unwrap(),
            vec![IpAddr::V6(Ipv6Addr::LOCALHOST)]
        );
    }
}
