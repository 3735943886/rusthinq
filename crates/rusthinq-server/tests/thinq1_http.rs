use openssl::{
    ssl::{SslConnector, SslMethod},
    x509::X509,
};
use rusthinq_protocol::lg_compat::TlsPolicy;
use rusthinq_server::{
    certificates::Authority,
    mqtt::{Clock, Sample},
    thinq1_http::{Config, Metadata, Service},
    tls::{Config as TlsConfig, FrontDoor},
};
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    task::JoinHandle,
    time::timeout,
};
use tokio_openssl::SslStream;
const PATH: &str = "/lgehadm/api/Device/TotalDeviceInfoSvc";
const PREFIX: &str = r#"<?xml version="1.0" encoding="utf-8" standalone="yes"?>"#;
struct FixedClock;
impl Clock for FixedClock {
    fn sample(&self) -> io::Result<Sample> {
        Ok(Sample {
            mid: 1_700_000_000_000,
            calendar: [0; 7],
        })
    }
}
fn authority() -> Arc<Authority> {
    static CA: OnceLock<Arc<Authority>> = OnceLock::new();
    CA.get_or_init(|| Arc::new(Authority::generate("root.example", 2048).unwrap()))
        .clone()
}
struct Harness {
    address: SocketAddr,
    metadata: mpsc::Receiver<Metadata>,
    stop: watch::Sender<bool>,
    task: JoinHandle<io::Result<()>>,
}
impl Harness {
    async fn start(config: Config) -> Self {
        let (service, metadata) = Service::new(config, Arc::new(FixedClock)).unwrap();
        let ca = authority();
        let identity = ca
            .server_identity("local.example", TlsPolicy::Baseline)
            .unwrap();
        let front = FrontDoor::new(TlsConfig::default(), vec![identity], None).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(front.serve_service(listener, Arc::new(service), stopped));
        Self {
            address,
            metadata,
            stop,
            task,
        }
    }
    async fn connect(&self) -> SslStream<TcpStream> {
        let mut client = SslConnector::builder(SslMethod::tls_client()).unwrap();
        client
            .cert_store_mut()
            .add_cert(X509::from_pem(authority().certificate_pem().as_bytes()).unwrap())
            .unwrap();
        let ssl = client
            .build()
            .configure()
            .unwrap()
            .into_ssl("local.example")
            .unwrap();
        let mut peer =
            SslStream::new(ssl, TcpStream::connect(self.address).await.unwrap()).unwrap();
        timeout(Duration::from_secs(3), Pin::new(&mut peer).connect())
            .await
            .unwrap()
            .unwrap();
        peer
    }
    async fn request(
        &self,
        method: &str,
        path: &str,
        headers: &str,
        body: &[u8],
    ) -> (u16, String, String) {
        let mut peer = self.connect().await;
        let head = format!(
            "{method} {path} HTTP/1.1\r\nHost: local.example\r\n{headers}Content-Length: {}\r\n\r\n",
            body.len()
        );
        peer.write_all(head.as_bytes()).await.unwrap();
        peer.write_all(body).await.unwrap();
        response(&mut peer).await
    }
    async fn shutdown(self) {
        self.stop.send_replace(true);
        timeout(Duration::from_secs(3), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
async fn response(peer: &mut SslStream<TcpStream>) -> (u16, String, String) {
    timeout(Duration::from_secs(3), async {
        let mut bytes = Vec::new();
        let header = loop {
            bytes.push(peer.read_u8().await.unwrap());
            assert!(bytes.len() < 8192);
            if bytes.ends_with(b"\r\n\r\n") {
                break String::from_utf8(bytes).unwrap();
            }
        };
        let status = header.split_whitespace().nth(1).unwrap().parse().unwrap();
        let mut length = 0;
        let mut kind = String::new();
        for line in header.lines() {
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    length = value.trim().parse().unwrap();
                }
                if name.eq_ignore_ascii_case("content-type") {
                    kind = value.trim().into();
                }
            }
        }
        let mut body = vec![0; length];
        peer.read_exact(&mut body).await.unwrap();
        (status, kind, String::from_utf8(body).unwrap())
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn metadata_entities_cdata_and_setting_response_preserve_wire_shape() {
    let mut harness = Harness::start(Config::default()).await;
    let body=br#"<lgedmRoot><modelName> Model&amp;&#x31;<![CDATA[&raw]]> </modelName><itemList><item>DM_SETTING_INFO_GET_URI</item></itemList><unknown>opaque</unknown></lgedmRoot>"#;
    let (status, kind, body) = harness
        .request(
            "POST",
            PATH,
            "X-LGEDM-DeviceId: device\r\nx-lgedm-devicetype: 201\r\n",
            body,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(kind, "text/xml;charset=utf-8");
    assert_eq!(
        body,
        format!(
            "{PREFIX}<lgedmRoot><returnCd>0000</returnCd><returnMsg>OK</returnMsg><itemList><elementList><elementCode>settingInfoList</elementCode><elementValueList><code>BlackBox</code><value>N</value></elementValueList></elementList><item>DM_SETTING_INFO_GET_URI</item><returnCode>0000</returnCode></itemList></lgedmRoot>"
        )
    );
    assert_eq!(
        harness.metadata.recv().await.unwrap(),
        Metadata {
            device_id: "device".into(),
            model_name: "Model&1&raw".into(),
            device_type: "201".into()
        }
    );
    harness.shutdown().await;
}
#[tokio::test]
async fn timesync_and_other_routes_match_reference_without_metadata_store() {
    let harness = Harness::start(Config::default()).await;
    let (_, _, body) = harness
        .request(
            "POST",
            PATH,
            "x-lgedm-deviceid: d\r\n",
            b"<lgedmRoot><itemList><item>THINQ_TIME_SYNC_URI</item></itemList></lgedmRoot>",
        )
        .await;
    assert_eq!(
        body,
        format!(
            "{PREFIX}<lgedmRoot><returnCd>0000</returnCd><returnMsg>OK</returnMsg><itemList><elementList><elementCode>utcTime</elementCode><elementValue>2023-11-14 22:13:20</elementValue></elementList><elementList><elementCode>timezone</elementCode><elementValue>0</elementValue></elementList><item>THINQ_TIME_SYNC_URI</item><returnCode>0000</returnCode></itemList></lgedmRoot>"
        )
    );
    assert_eq!(
        harness
            .request("POST", "/lgehadm/api/Grid/PowerSavingInfoSvc", "", b"")
            .await
            .2,
        format!(
            "{PREFIX}<lgedmRoot><returnCd>0108</returnCd><returnMsg>No Saving Data.</returnMsg></lgedmRoot>"
        )
    );
    for path in [PATH, "/lgehadm/api/Rtos/FWInfoSettingSvc"] {
        assert_eq!(
            harness
                .request("POST", path, "x-lgedm-deviceid: d\r\n", b"<lgedmRoot/>")
                .await
                .2,
            format!(
                "{PREFIX}<lgedmRoot><returnCd>0000</returnCd><returnMsg>OK</returnMsg></lgedmRoot>"
            )
        );
    }
    assert_eq!(
        harness
            .request("POST", "/lgehadm/report/diagmon", "", b"")
            .await
            .0,
        200
    );
    assert_eq!(harness.request("GET", PATH, "", b"").await.0, 405);
    assert_eq!(
        harness.request("GET", "/unknown", "", b"").await,
        (200, "application/json".into(), "{}".into())
    );
    harness.shutdown().await;
}
#[tokio::test]
async fn malformed_xml_and_field_limits_do_not_publish_metadata() {
    let mut harness = Harness::start(Config::default()).await;
    for body in [
        b"not xml".as_slice(),
        b"<lgedmRoot><modelName>M</lgedmRoot>",
        b"<!DOCTYPE lgedmRoot [<!ENTITY x SYSTEM 'file:///unused'>]><lgedmRoot/>",
        b"<lgedmRoot><modelName>A</modelName><modelName>B</modelName></lgedmRoot>",
        b"<lgedmRoot><modelName>&unknown;</modelName></lgedmRoot>",
        b"<lgedmRoot><modelName>&#0;</modelName></lgedmRoot>",
        b"<lgedmRoot><modelName>A<nested/>B</modelName></lgedmRoot>",
        b"<lgedmRoot/><lgedmRoot/>",
        b"<lgedmRoot><modelName>\xff</modelName></lgedmRoot>",
    ] {
        assert_eq!(
            harness
                .request(
                    "POST",
                    PATH,
                    "x-lgedm-deviceid: d\r\nx-lgedm-devicetype: 1\r\n",
                    body
                )
                .await
                .0,
            400
        );
    }
    let large = format!(
        "<lgedmRoot><modelName>{}</modelName></lgedmRoot>",
        "x".repeat(1025)
    );
    assert_eq!(
        harness
            .request("POST", PATH, "x-lgedm-deviceid: d\r\n", large.as_bytes())
            .await
            .0,
        400
    );
    assert_eq!(
        harness.request("POST", PATH, "", b"<lgedmRoot/>").await.0,
        400
    );
    assert!(harness.metadata.try_recv().is_err());
    harness.shutdown().await;
}
#[tokio::test]
async fn metadata_backpressure_and_disabled_receiver_are_explicit() {
    let mut harness = Harness::start(Config {
        metadata_capacity: 1,
        ..Config::default()
    })
    .await;
    let body = b"<lgedmRoot><modelName>M</modelName></lgedmRoot>";
    let header = "x-lgedm-deviceid: d\r\nx-lgedm-devicetype: 1\r\n";
    assert_eq!(harness.request("POST", PATH, header, body).await.0, 200);
    assert_eq!(harness.request("POST", PATH, header, body).await.0, 503);
    assert_eq!(
        harness
            .request("POST", "/lgehadm/report/diagmon", "", b"")
            .await
            .0,
        200
    );
    harness.metadata.recv().await.unwrap();
    assert_eq!(harness.request("POST", PATH, header, body).await.0, 200);
    harness.metadata.close();
    assert_eq!(harness.request("POST", PATH, header, body).await.0, 503);
    harness.shutdown().await;
}
#[tokio::test]
async fn body_bounds_timeout_and_shutdown_are_enforced() {
    let harness = Harness::start(Config {
        max_body: 128,
        request_timeout: Duration::from_millis(200),
        ..Config::default()
    })
    .await;
    assert_eq!(
        harness
            .request("POST", PATH, "x-lgedm-deviceid: d\r\n", &[b'x'; 129])
            .await
            .0,
        413
    );
    let mut peer = harness.connect().await;
    peer.write_all(format!("POST {PATH} HTTP/1.1\r\nHost: local.example\r\nx-lgedm-deviceid: d\r\nTransfer-Encoding: chunked\r\n\r\n81\r\n").as_bytes()).await.unwrap();
    peer.write_all(&[b'x'; 129]).await.unwrap();
    peer.write_all(b"\r\n0\r\n\r\n").await.unwrap();
    assert_eq!(response(&mut peer).await.0, 413);
    let mut peer = harness.connect().await;
    peer.write_all(format!("POST {PATH} HTTP/1.1\r\nHost: local.example\r\nx-lgedm-deviceid: d\r\nContent-Length: 20\r\n\r\n").as_bytes()).await.unwrap();
    assert_eq!(response(&mut peer).await.0, 408);
    let mut pending = harness.connect().await;
    pending.write_all(b"POST ").await.unwrap();
    harness.shutdown().await;
}
