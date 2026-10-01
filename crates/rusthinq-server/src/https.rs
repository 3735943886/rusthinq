//! Shared HTTPS endpoint for ThinQ1 XML and ThinQ2 provisioning.
use crate::{
    provisioning, thinq1_http,
    tls::{LocalFuture, LocalService, Transport},
};
use hyper::{server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{convert::Infallible, io, time::Duration};
use tokio::time::timeout;

#[derive(Clone)]
pub struct Service {
    thin: thinq1_http::Service,
    provisioning: provisioning::Service,
    headers: Duration,
    connection: Duration,
}
impl Service {
    pub fn new(
        thin: thinq1_http::Service,
        provisioning: provisioning::Service,
        headers: Duration,
        connection: Duration,
    ) -> io::Result<Self> {
        if headers.is_zero() || connection.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "zero HTTPS timeout",
            ));
        }
        Ok(Self {
            thin,
            provisioning,
            headers,
            connection,
        })
    }
}
impl LocalService for Service {
    fn serve(&self, stream: Transport) -> LocalFuture {
        let service = self.clone();
        Box::pin(async move {
            let mut builder = http1::Builder::new();
            builder
                .timer(TokioTimer::new())
                .header_read_timeout(service.headers)
                .max_headers(32)
                .max_buf_size(8192)
                .keep_alive(false);
            let deadline = service.connection;
            let handler = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                let service = service.clone();
                async move {
                    let response = if request.uri().path().starts_with("/lgehadm/") {
                        service.thin.request(request).await
                    } else {
                        service.provisioning.request(request).await
                    };
                    Ok::<_, Infallible>(response)
                }
            });
            timeout(
                deadline,
                builder.serve_connection(TokioIo::new(stream), handler),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "HTTPS connection timeout"))?
            .map_err(io::Error::other)
        })
    }
}
