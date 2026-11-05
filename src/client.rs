//! Connection handling: connects to the target and sends single requests.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::uri::{Authority, PathAndQuery, Scheme};
use http::{HeaderValue, Request, Uri, Version, header};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::{http1, http2};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

use crate::Protocol;

/// Where requests go, parsed once from the configured target address.
pub(crate) struct Target {
    protocol: Protocol,
    scheme: Scheme,
    authority: Authority,
    host: String,
    port: u16,
    base_path: String,
    tls: Option<TlsConnector>,
}

impl Target {
    pub(crate) fn new(target: &str, protocol: Protocol) -> Self {
        let uri: Uri = target
            .parse()
            .unwrap_or_else(|e| panic!("gumo: invalid target address {target:?}: {e}"));
        let scheme = uri.scheme().cloned().unwrap_or(Scheme::HTTP);
        let authority = uri
            .authority()
            .cloned()
            .unwrap_or_else(|| panic!("gumo: target address {target:?} has no host"));
        let https = if scheme == Scheme::HTTPS {
            true
        } else if scheme == Scheme::HTTP {
            false
        } else {
            panic!("gumo: target address {target:?} is neither http nor https");
        };
        let host = authority
            .host()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        let port = authority.port_u16().unwrap_or(if https { 443 } else { 80 });
        let tls = https.then(|| {
            let mut roots = RootCertStore::empty();
            roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
            let mut config = ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            config.alpn_protocols = vec![match protocol {
                Protocol::Http1 => b"http/1.1".to_vec(),
                Protocol::Http2 => b"h2".to_vec(),
            }];
            TlsConnector::from(Arc::new(config))
        });
        Target {
            protocol,
            scheme,
            authority,
            host,
            port,
            base_path: uri.path().trim_end_matches('/').to_string(),
            tls,
        }
    }

    /// Points a request built against a path (`/hello?x=1`) at the target.
    pub(crate) fn prepare(&self, request: Request<Bytes>) -> Request<Full<Bytes>> {
        let (mut parts, body) = request.into_parts();
        let path = parts.uri.path_and_query().map_or("/", PathAndQuery::as_str);
        let path: PathAndQuery = format!("{}{path}", self.base_path)
            .parse()
            .unwrap_or_else(|e| panic!("gumo: invalid request path {path:?}: {e}"));
        match self.protocol {
            Protocol::Http1 => {
                parts.version = Version::HTTP_11;
                parts.uri = Uri::from(path);
                parts.headers.entry(header::HOST).or_insert_with(|| {
                    HeaderValue::from_str(self.authority.as_str())
                        .expect("gumo: target authority is a valid header")
                });
            }
            Protocol::Http2 => {
                parts.version = Version::HTTP_2;
                parts.uri = Uri::builder()
                    .scheme(self.scheme.clone())
                    .authority(self.authority.clone())
                    .path_and_query(path)
                    .build()
                    .expect("gumo: cannot build the request URI");
                // HTTP/2 carries the host in :authority.
                parts.headers.remove(header::HOST);
            }
        }
        Request::from_parts(parts, Full::new(body))
    }

    async fn connect(&self) -> Result<Sender, String> {
        let tcp = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(|e| format!("connect: {e}"))?;
        tcp.set_nodelay(true).map_err(|e| format!("connect: {e}"))?;
        match &self.tls {
            None => self.handshake(tcp).await,
            Some(tls) => {
                let name = ServerName::try_from(self.host.clone())
                    .map_err(|e| format!("tls: invalid server name: {e}"))?;
                let stream = tls
                    .connect(name, tcp)
                    .await
                    .map_err(|e| format!("tls: {e}"))?;
                self.handshake(stream).await
            }
        }
    }

    async fn handshake<IO>(&self, io: IO) -> Result<Sender, String>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let io = TokioIo::new(io);
        let handshake_error = |e: hyper::Error| format!("handshake: {e}");
        match self.protocol {
            Protocol::Http1 => {
                let (sender, connection) = http1::handshake(io).await.map_err(handshake_error)?;
                tokio::spawn(connection);
                Ok(Sender::Http1(sender))
            }
            Protocol::Http2 => {
                let (sender, connection) = http2::handshake(TokioExecutor::new(), io)
                    .await
                    .map_err(handshake_error)?;
                tokio::spawn(connection);
                Ok(Sender::Http2(sender))
            }
        }
    }
}

enum Sender {
    Http1(http1::SendRequest<Full<Bytes>>),
    Http2(http2::SendRequest<Full<Bytes>>),
}

impl Sender {
    fn is_closed(&self) -> bool {
        match self {
            Sender::Http1(sender) => sender.is_closed(),
            Sender::Http2(sender) => sender.is_closed(),
        }
    }

    async fn ready(&mut self) -> hyper::Result<()> {
        match self {
            Sender::Http1(sender) => sender.ready().await,
            Sender::Http2(sender) => sender.ready().await,
        }
    }

    async fn send(
        &mut self,
        request: Request<Full<Bytes>>,
    ) -> hyper::Result<http::Response<hyper::body::Incoming>> {
        match self {
            Sender::Http1(sender) => sender.send_request(request).await,
            Sender::Http2(sender) => sender.send_request(request).await,
        }
    }
}

/// An HTTP/2 connection shared by the streams that multiplex over it.
pub(crate) type SharedConnection = Arc<Mutex<Option<http2::SendRequest<Full<Bytes>>>>>;

pub(crate) struct Outcome {
    pub(crate) duration: Duration,
    pub(crate) result: Result<Response, String>,
}

pub(crate) struct Response {
    pub(crate) status: u16,
    pub(crate) body_len: u64,
}

/// One virtual user: sends one request at a time over its connection.
pub(crate) struct Client {
    target: Arc<Target>,
    /// Some for HTTP/2, where streams share a connection; HTTP/1 connections are private.
    shared: Option<SharedConnection>,
    sender: Option<Sender>,
    timeout: Duration,
}

impl Client {
    pub(crate) fn new(
        target: Arc<Target>,
        shared: Option<SharedConnection>,
        timeout: Duration,
    ) -> Self {
        Client {
            target,
            shared,
            sender: None,
            timeout,
        }
    }

    /// Opens the connection if there is none, or the old one was closed.
    pub(crate) async fn connect(&mut self) -> Result<(), String> {
        if self
            .sender
            .as_ref()
            .is_some_and(|sender| !sender.is_closed())
        {
            return Ok(());
        }
        self.sender = None;
        let connect = async {
            match &self.shared {
                None => self.target.connect().await,
                Some(shared) => {
                    let mut shared = shared.lock().await;
                    if let Some(sender) = shared.as_ref().filter(|sender| !sender.is_closed()) {
                        return Ok(Sender::Http2(sender.clone()));
                    }
                    let Sender::Http2(sender) = self.target.connect().await? else {
                        unreachable!("an HTTP/2 target opened an HTTP/1 connection");
                    };
                    *shared = Some(sender.clone());
                    Ok(Sender::Http2(sender))
                }
            }
        };
        let sender = tokio::time::timeout(self.timeout, connect)
            .await
            .map_err(|_| "connect: timed out".to_string())??;
        self.sender = Some(sender);
        Ok(())
    }

    pub(crate) async fn send(&mut self, request: Request<Full<Bytes>>) -> Outcome {
        if let Err(e) = self.connect().await {
            return Outcome {
                duration: Duration::ZERO,
                result: Err(e),
            };
        }
        let sender = self.sender.as_mut().expect("connected above");
        // Waiting for a free HTTP/2 stream is not part of the request duration.
        if let Err(e) = sender.ready().await {
            self.sender = None;
            return Outcome {
                duration: Duration::ZERO,
                result: Err(e.to_string()),
            };
        }
        let start = Instant::now();
        let exchange = async {
            let response = sender.send(request).await?;
            let status = response.status().as_u16();
            let mut body = response.into_body();
            let mut body_len = 0;
            while let Some(frame) = body.frame().await {
                if let Ok(data) = frame?.into_data() {
                    body_len += data.len() as u64;
                }
            }
            Ok::<_, hyper::Error>(Response { status, body_len })
        };
        let result = match tokio::time::timeout(self.timeout, exchange).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err("request timed out".to_string()),
        };
        let duration = start.elapsed();
        if result.is_err() && matches!(self.sender, Some(Sender::Http1(_))) {
            // An HTTP/1 connection is unusable after an aborted exchange.
            self.sender = None;
        }
        Outcome { duration, result }
    }
}
