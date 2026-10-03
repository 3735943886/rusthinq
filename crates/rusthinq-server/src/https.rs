//! Shared HTTPS endpoint for ThinQ1 XML and ThinQ2 provisioning.
use crate::{
    provisioning, thinq1_http,
    tls::{LocalFuture, LocalService, Transport},
};
use hyper::{server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{convert::Infallible, io, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    sync::watch,
    task::JoinSet,
    time::timeout,
};

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
impl Service {
    fn connection<S>(&self, stream: S) -> LocalFuture
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
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
    /// Plain HTTP with the same routes, for 0.1's `http_port` (a TLS-terminating reverse
    /// proxy in front) and ThinQ1 `thinq1_https_port` listeners. At most `max` connections
    /// are served at once; further peers are closed on accept.
    pub async fn serve_plain(
        self,
        listener: TcpListener,
        max: usize,
        mut stop: watch::Receiver<bool>,
    ) -> io::Result<()> {
        let mut connections = JoinSet::new();
        let result = loop {
            if *stop.borrow() {
                break Ok(());
            }
            tokio::select! {
                biased;
                _ = stop.changed() => break Ok(()),
                _ = connections.join_next(), if !connections.is_empty() => {}
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => {
                        if connections.len() < max {
                            connections.spawn(self.connection(stream));
                        }
                    }
                    Err(error) => break Err(error),
                }
            }
        };
        drop(listener);
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        result
    }
}
impl LocalService for Service {
    fn serve(&self, stream: Transport) -> LocalFuture {
        self.connection(stream)
    }
}
