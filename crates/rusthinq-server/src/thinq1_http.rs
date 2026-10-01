//! ThinQ1 XML endpoints. Metadata is an observation for L6, never a local device registry.
use crate::{
    mqtt::Clock,
    tls::{LocalFuture, LocalService, Transport},
};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use quick_xml::{Reader, events::Event};
use std::{convert::Infallible, io, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::timeout};
const XML_HEADER: &str = r#"<?xml version="1.0" encoding="utf-8" standalone="yes"?>"#;
const OK: &str = "<returnCd>0000</returnCd><returnMsg>OK</returnMsg>";
#[derive(Clone, Debug)]
pub struct Config {
    pub max_body: usize,
    pub metadata_capacity: usize,
    pub request_timeout: Duration,
    pub connection_timeout: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            max_body: 1_000_000,
            metadata_capacity: 64,
            request_timeout: Duration::from_secs(10),
            connection_timeout: Duration::from_secs(30),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    pub device_id: String,
    pub model_name: String,
    pub device_type: String,
}
#[derive(Clone)]
pub struct Service {
    config: Config,
    clock: Arc<dyn Clock>,
    metadata: mpsc::Sender<Metadata>,
}
impl Service {
    pub fn new(
        config: Config,
        clock: Arc<dyn Clock>,
    ) -> io::Result<(Self, mpsc::Receiver<Metadata>)> {
        if config.max_body == 0
            || config.max_body > 1_000_000
            || config.metadata_capacity == 0
            || config.request_timeout.is_zero()
            || config.connection_timeout.is_zero()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid ThinQ1 HTTP configuration",
            ));
        }
        let (metadata, receive) = mpsc::channel(config.metadata_capacity);
        Ok((
            Self {
                config,
                clock,
                metadata,
            },
            receive,
        ))
    }
    pub(crate) async fn request(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let path = request.uri().path().to_owned();
        let known = matches!(
            path.as_str(),
            "/lgehadm/api/Device/TotalDeviceInfoSvc"
                | "/lgehadm/api/Grid/PowerSavingInfoSvc"
                | "/lgehadm/api/Rtos/FWInfoSettingSvc"
                | "/lgehadm/report/diagmon"
        );
        if !known {
            return response(StatusCode::OK, "application/json", "{}".into());
        }
        if request.method() != Method::POST {
            return response(StatusCode::METHOD_NOT_ALLOWED, "text/plain", String::new());
        }
        let total = path == "/lgehadm/api/Device/TotalDeviceInfoSvc";
        let device_id = field(&request, "x-lgedm-deviceid", 256);
        let device_type = field(&request, "x-lgedm-devicetype", 128);
        if total
            && (device_id.is_none()
                || (request.headers().contains_key("x-lgedm-devicetype") && device_type.is_none()))
        {
            return empty(StatusCode::BAD_REQUEST);
        }
        if request
            .headers()
            .get(hyper::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .is_some_and(|n| n > self.config.max_body as u64)
        {
            return empty(StatusCode::PAYLOAD_TOO_LARGE);
        }
        let body = match timeout(
            self.config.request_timeout,
            Limited::new(request.into_body(), self.config.max_body).collect(),
        )
        .await
        {
            Ok(Ok(body)) => body.to_bytes(),
            Ok(Err(error)) => {
                return empty(if error.is::<http_body_util::LengthLimitError>() {
                    StatusCode::PAYLOAD_TOO_LARGE
                } else {
                    StatusCode::BAD_REQUEST
                });
            }
            Err(_) => return empty(StatusCode::REQUEST_TIMEOUT),
        };
        match path.as_str() {
            "/lgehadm/api/Grid/PowerSavingInfoSvc" => {
                xml("<returnCd>0108</returnCd><returnMsg>No Saving Data.</returnMsg>")
            }
            "/lgehadm/api/Rtos/FWInfoSettingSvc" => xml(OK),
            "/lgehadm/report/diagmon" => empty(StatusCode::OK),
            _ => {
                let parsed = match parse(&body) {
                    Ok(parsed) => parsed,
                    Err(()) => return empty(StatusCode::BAD_REQUEST),
                };
                // Construct all fallible response material before admitting metadata.
                let inner = match parsed.item.as_deref() {
                    Some("DM_SETTING_INFO_GET_URI") => format!(
                        "{OK}<itemList><elementList><elementCode>settingInfoList</elementCode><elementValueList><code>BlackBox</code><value>N</value></elementValueList></elementList><item>DM_SETTING_INFO_GET_URI</item><returnCode>0000</returnCode></itemList>"
                    ),
                    Some("THINQ_TIME_SYNC_URI") => {
                        let sample = match self.clock.sample() {
                            Ok(sample) => sample,
                            Err(_) => return empty(StatusCode::SERVICE_UNAVAILABLE),
                        };
                        let timestamp = match i64::try_from(sample.mid)
                            .ok()
                            .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis)
                        {
                            Some(timestamp) => timestamp,
                            None => return empty(StatusCode::SERVICE_UNAVAILABLE),
                        };
                        format!(
                            "{OK}<itemList><elementList><elementCode>utcTime</elementCode><elementValue>{}</elementValue></elementList><elementList><elementCode>timezone</elementCode><elementValue>0</elementValue></elementList><item>THINQ_TIME_SYNC_URI</item><returnCode>0000</returnCode></itemList>",
                            timestamp.format("%Y-%m-%d %H:%M:%S")
                        )
                    }
                    _ => OK.into(),
                };
                if let (Some(model_name), Some(device_type)) = (parsed.model, device_type)
                    && self
                        .metadata
                        .try_send(Metadata {
                            device_id: device_id.expect("validated identity"),
                            model_name,
                            device_type,
                        })
                        .is_err()
                {
                    return empty(StatusCode::SERVICE_UNAVAILABLE);
                }
                xml(&inner)
            }
        }
    }
}
impl LocalService for Service {
    fn serve(&self, stream: Transport) -> LocalFuture {
        let service = self.clone();
        let config = self.config.clone();
        Box::pin(async move {
            let mut builder = http1::Builder::new();
            builder
                .timer(TokioTimer::new())
                .header_read_timeout(config.request_timeout)
                .max_headers(32)
                .max_buf_size(8192)
                .keep_alive(false);
            let handler = service_fn(move |request| {
                let service = service.clone();
                async move { Ok::<_, Infallible>(service.request(request).await) }
            });
            timeout(
                config.connection_timeout,
                builder.serve_connection(TokioIo::new(stream), handler),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "ThinQ1 HTTP connection timeout"))?
            .map_err(io::Error::other)
        })
    }
}
fn field(request: &Request<Incoming>, name: &str, maximum: usize) -> Option<String> {
    request
        .headers()
        .get(name)?
        .to_str()
        .ok()
        .filter(|value| {
            !value.is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
        })
        .map(str::to_owned)
}
fn empty(status: StatusCode) -> Response<Full<Bytes>> {
    response(status, "text/plain", String::new())
}
fn response(status: StatusCode, kind: &str, body: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, kind)
        .body(Full::new(Bytes::from(body)))
        .expect("static response")
}
fn xml(inner: &str) -> Response<Full<Bytes>> {
    response(
        StatusCode::OK,
        "text/xml;charset=utf-8",
        format!("{XML_HEADER}<lgedmRoot>{inner}</lgedmRoot>"),
    )
}
#[derive(Default)]
struct Parsed {
    model: Option<String>,
    item: Option<String>,
}
fn parse(bytes: &[u8]) -> Result<Parsed, ()> {
    let text = std::str::from_utf8(bytes).map_err(|_| ())?;
    if text.chars().any(|character| !xml_character(character)) {
        return Err(());
    }
    let mut reader = Reader::from_str(text);
    reader.config_mut().expand_empty_elements = true;
    let mut seen = [false; 2];
    let mut stack: Vec<Vec<u8>> = Vec::new();
    let mut parsed = Parsed::default();
    let mut value = String::new();
    let mut root = false;
    loop {
        match reader.read_event().map_err(|_| ())? {
            Event::Start(element) => {
                if target(&stack).is_some() {
                    return Err(());
                }
                for attribute in element.attributes() {
                    let attribute = attribute.map_err(|_| ())?;
                    attribute
                        .decode_and_unescape_value(reader.decoder())
                        .map_err(|_| ())?;
                }
                if stack.is_empty() {
                    if root || element.name().as_ref() != b"lgedmRoot" {
                        return Err(());
                    }
                    root = true;
                }
                if stack.len() == 16 {
                    return Err(());
                }
                stack.push(element.name().as_ref().to_vec());
                if let Some(model) = target(&stack) {
                    let index = usize::from(!model);
                    if seen[index] {
                        return Err(());
                    }
                    seen[index] = true;
                    value.clear();
                }
            }
            Event::Empty(_) => return Err(()),
            Event::Text(text) => {
                let decoded = text.decode().map_err(|_| ())?;
                append(&mut value, &stack, &decoded)?;
            }
            Event::CData(text) => {
                let decoded = text.decode().map_err(|_| ())?;
                if stack.is_empty() {
                    return Err(());
                }
                append(&mut value, &stack, &decoded)?;
            }
            Event::GeneralRef(reference) => {
                let encoded = format!("&{};", reference.decode().map_err(|_| ())?);
                let decoded = quick_xml::escape::unescape(&encoded).map_err(|_| ())?;
                append(&mut value, &stack, &decoded)?;
            }
            Event::End(element) => {
                if stack.last().map(Vec::as_slice) != Some(element.name().as_ref()) {
                    return Err(());
                }
                if let Some(model) = target(&stack) {
                    let slot = if model {
                        &mut parsed.model
                    } else {
                        &mut parsed.item
                    };
                    if slot.is_some() {
                        return Err(());
                    }
                    let text = value.trim().to_owned();
                    if !text.is_empty() {
                        *slot = Some(text);
                    }
                }
                stack.pop();
            }
            Event::DocType(_) => return Err(()),
            Event::Eof => {
                return if root && stack.is_empty() {
                    Ok(parsed)
                } else {
                    Err(())
                };
            }
            _ => {}
        }
    }
}
fn xml_character(character: char) -> bool {
    matches!(character as u32, 9 | 10 | 13 | 0x20..=0xd7ff | 0xe000..=0xfffd | 0x10000..=0x10ffff)
}
fn append(value: &mut String, stack: &[Vec<u8>], decoded: &str) -> Result<(), ()> {
    if decoded.chars().any(|character| !xml_character(character)) {
        return Err(());
    }
    if target(stack).is_some() {
        if value.len() + decoded.len() > 1024 {
            return Err(());
        }
        value.push_str(decoded);
    } else if stack.is_empty() && !decoded.trim().is_empty() {
        return Err(());
    }
    Ok(())
}
fn target(stack: &[Vec<u8>]) -> Option<bool> {
    if stack.len() == 2 && stack[0] == b"lgedmRoot" && stack[1] == b"modelName" {
        Some(true)
    } else if stack.len() == 3
        && stack[0] == b"lgedmRoot"
        && stack[1] == b"itemList"
        && stack[2] == b"item"
    {
        Some(false)
    } else {
        None
    }
}
