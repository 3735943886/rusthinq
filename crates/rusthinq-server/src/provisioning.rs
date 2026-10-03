//! ThinQ2 HTTPS provisioning. Configuration and CA/signer ownership are caller supplied.
use crate::{
    certificates::{Authority, SignError, SigningHandle},
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
use rusthinq_protocol::client_hello::hostname;
use serde_json::{Value, json};
use std::{convert::Infallible, io, sync::Arc, time::Duration};
use tokio::time::timeout;

#[derive(Clone, Debug)]
pub struct Config {
    pub hostname: String,
    pub https_port: u16,
    pub mqtts_port: u16,
    pub advertise_requested_host: bool,
    /// Verbatim `/route` endpoints (0.1 `advertise = "<url>"`), e.g. a reverse proxy.
    pub api_server: Option<String>,
    pub mqtt_server: Option<String>,
    pub max_body: usize,
    pub request_timeout: Duration,
    pub connection_timeout: Duration,
}
impl Config {
    pub fn new(hostname: String) -> Self {
        Self {
            hostname,
            https_port: 443,
            mqtts_port: 8883,
            advertise_requested_host: false,
            api_server: None,
            mqtt_server: None,
            max_body: 65536,
            request_timeout: Duration::from_secs(10),
            connection_timeout: Duration::from_secs(30),
        }
    }
}
#[derive(Clone)]
pub struct Service {
    state: Arc<State>,
}
struct State {
    config: Config,
    root: String,
    signer: SigningHandle,
}
impl Service {
    /// Custom root only changes the trust material served by /route/certificate.
    /// Device certificates continue to use the supplied signing worker's CA.
    pub fn new(
        config: Config,
        authority: &Authority,
        signer: SigningHandle,
        custom_root: Option<&[u8]>,
    ) -> io::Result<Self> {
        if !hostname(&config.hostname)
            || config.https_port == 0
            || config.mqtts_port == 0
            || config.max_body == 0
            || config.max_body > 65536
            || config.request_timeout.is_zero()
            || config.connection_timeout.is_zero()
            || [&config.api_server, &config.mqtt_server]
                .into_iter()
                .flatten()
                .any(|url| url.is_empty() || url.len() > 2048 || url.chars().any(char::is_control))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid provisioning configuration",
            ));
        }
        let root = if let Some(root) = custom_root {
            if root.len() > 262144
                || openssl::x509::X509::stack_from_pem(root)
                    .map_err(io::Error::other)?
                    .is_empty()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid custom root",
                ));
            }
            std::str::from_utf8(root)
                .map_err(io::Error::other)?
                .to_owned()
        } else {
            authority.certificate_pem().to_owned()
        };
        if root.len() > 262144 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "root exceeded"));
        }
        Ok(Self {
            state: Arc::new(State {
                config,
                root,
                signer,
            }),
        })
    }
    pub(crate) async fn request(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let path = request.uri().path();
        match path {
            "/route" => {
                if request.method() != Method::GET {
                    return empty(StatusCode::METHOD_NOT_ALLOWED);
                }
                let requested = request
                    .headers()
                    .get(hyper::header::HOST)
                    .and_then(|value| value.to_str().ok())
                    .and_then(host_header);
                let config = &self.state.config;
                let name = if config.advertise_requested_host {
                    requested.unwrap_or(&config.hostname)
                } else {
                    &config.hostname
                };
                json_response(
                    StatusCode::OK,
                    json!({"resultCode":"0000","result":{
                        "apiServer": config.api_server.clone().unwrap_or_else(|| endpoint("https", name, config.https_port)),
                        "mqttServer": config.mqtt_server.clone().unwrap_or_else(|| endpoint("ssl", name, config.mqtts_port))
                    }}),
                )
            }
            "/route/certificate" => {
                if request.method() != Method::GET {
                    return empty(StatusCode::METHOD_NOT_ALLOWED);
                }
                let named = request.uri().query().is_some_and(|query| {
                    query.split('&').any(|pair| {
                        let key = pair.split('=').next().unwrap_or("");
                        percent_encoding::percent_decode_str(key)
                            .decode_utf8()
                            .is_ok_and(|key| key == "name")
                    })
                });
                let result = if named {
                    json!({"certificatePem":self.state.root})
                } else {
                    json!(["common-server", "aws-iot"])
                };
                json_response(StatusCode::OK, json!({"resultCode":"0000","result":result}))
            }
            _ if path.starts_with("/device/") && path.ends_with("/certificate") => {
                if request.method() != Method::POST {
                    return empty(StatusCode::METHOD_NOT_ALLOWED);
                }
                let Some(encoded) = path.get(8..path.len() - 12) else {
                    return empty(StatusCode::BAD_REQUEST);
                };
                let device = match percent_encoding::percent_decode_str(encoded).decode_utf8() {
                    Ok(device)
                        if !device.is_empty()
                            && device.len() <= 256
                            && !device.contains(['/', '+', '#', '\0']) =>
                    {
                        device.into_owned()
                    }
                    _ => return empty(StatusCode::BAD_REQUEST),
                };
                if !request
                    .headers()
                    .get(hyper::header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|value| {
                        let mime = value.split(';').next().unwrap_or("").trim();
                        mime.eq_ignore_ascii_case("application/json")
                            || (mime.starts_with("application/") && mime.ends_with("+json"))
                    })
                {
                    return empty(StatusCode::UNSUPPORTED_MEDIA_TYPE);
                }
                if request
                    .headers()
                    .get(hyper::header::CONTENT_LENGTH)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
                    .is_some_and(|length| length > self.state.config.max_body as u64)
                {
                    return empty(StatusCode::PAYLOAD_TOO_LARGE);
                }
                let bytes = match timeout(
                    self.state.config.request_timeout,
                    Limited::new(request.into_body(), self.state.config.max_body).collect(),
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
                let body: Value = match serde_json::from_slice(&bytes) {
                    Ok(value) => value,
                    Err(_) => return empty(StatusCode::BAD_REQUEST),
                };
                let Some(csr) = body.get("csr").and_then(Value::as_str) else {
                    return empty(StatusCode::UNPROCESSABLE_ENTITY);
                };
                match timeout(
                    self.state.config.request_timeout,
                    self.state.signer.sign(&device, csr.as_bytes()),
                )
                .await
                {
                    Ok(Ok(pem)) => json_response(
                        StatusCode::OK,
                        json!({"resultCode":"0000","result":{"certificatePem":pem}}),
                    ),
                    Ok(Err(SignError::Busy | SignError::Stopped)) => json_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        json!({"resultCode":"9999"}),
                    ),
                    _ => json_response(StatusCode::OK, json!({"resultCode":"9999"})),
                }
            }
            _ => Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::CONTENT_TYPE, "text/xml;charset=utf-8")
                .body(Full::new(Bytes::new()))
                .expect("static response"),
        }
    }
}
impl LocalService for Service {
    fn serve(&self, stream: Transport) -> LocalFuture {
        let service = self.clone();
        let config = self.state.config.clone();
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
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "HTTP connection timeout"))?
            .map_err(io::Error::other)
        })
    }
}
fn host_header(host: &str) -> Option<&str> {
    let name = if let Some((name, port)) = host.rsplit_once(':') {
        port.parse::<u16>().ok().filter(|port| *port != 0)?;
        name
    } else {
        host
    };
    hostname(name).then_some(name)
}
fn endpoint(scheme: &str, host: &str, port: u16) -> String {
    format!("{scheme}://{host}:{port}")
}
fn empty(status: StatusCode) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .expect("static response")
}
fn json_response(status: StatusCode, value: Value) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(value.to_string())))
        .expect("static response")
}
