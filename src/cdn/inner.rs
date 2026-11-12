// Modified by Aurelia; see README.
use reqwest::{Client, Response};
use std::sync::{Arc, OnceLock};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;
use steam_vent::{Connection, ConnectionTrait};
use steam_vent_proto::steammessages_clientserver_appinfo::{
    cmsg_client_picsproduct_info_request::AppInfo, CMsgClientPICSAccessTokenRequest,
    CMsgClientPICSAccessTokenResponse, CMsgClientPICSProductInfoRequest,
    CMsgClientPICSProductInfoResponse,
};

use crate::{web_api::content_service::CDNServer, Error};

use super::depot_chunk;
use super::throttle::RateLimiter;

/// Tries per chunk, across the pool.
pub(crate) const CHUNK_ATTEMPTS: u32 = 5;
/// Largest chunk body accepted.
pub(crate) const MAX_CHUNK_BODY: usize = crate::utils::lzma::MAX_CHUNK_LEN + (1 << 20);
const _: () = assert!(MAX_CHUNK_BODY > crate::utils::lzma::MAX_CHUNK_LEN);
/// Largest manifest body accepted.
pub(crate) const MAX_MANIFEST_BODY: usize = 512 << 20;
/// Failures before a host is benched.
pub(crate) const BENCH_STRIKES: u32 = 3;

#[derive(Debug)]
pub(crate) struct InnerClient {
    pub connection: Arc<Connection>,
    web_client: Client,
    pub servers: Vec<CDNServer>,
    /// Recent failures per server.
    strikes: Vec<AtomicU32>,
    /// Round-robin cursor.
    next: AtomicUsize,
    /// Optional shared rate limit.
    limiter: OnceLock<Arc<RateLimiter>>,
}

/// Index to use, skipping benched hosts.
pub(crate) fn pick_index(strikes: &[u32], start: usize) -> Option<usize> {
    let n = strikes.len();
    if n == 0 {
        return None;
    }
    (0..n)
        .map(|i| (start + i) % n)
        .find(|&i| strikes[i] < BENCH_STRIKES)
        // All benched: least-failing one.
        .or_else(|| (0..n).min_by_key(|&i| strikes[i]))
}

/// Delay before retry `attempt` (0-based).
pub(crate) fn backoff(attempt: u32) -> Duration {
    Duration::from_millis((200u64 << attempt.min(4)).min(3_000))
}

/// Match a chunk's size and SHA-1.
pub(crate) fn check_chunk(
    chunk_id: &str,
    data: Vec<u8>,
    expected_sha: &[u8],
    expected_len: u32,
) -> Result<Vec<u8>, Error> {
    use sha1::Digest;
    if data.len() != expected_len as usize {
        return Err(Error::Decompress(format!(
            "chunk {chunk_id}: {} bytes, manifest says {expected_len}",
            data.len()
        )));
    }
    // Empty hash: nothing to compare.
    if !expected_sha.is_empty() && sha1::Sha1::digest(&data).as_slice() != expected_sha {
        return Err(Error::Decompress(format!("chunk {chunk_id}: SHA-1 mismatch")));
    }
    Ok(data)
}

/// Whether another try may succeed.
pub(crate) fn retryable(err: &Error) -> bool {
    matches!(
        err,
        Error::Request(_)
            | Error::HttpStatus(_)
            | Error::Network(_)
            | Error::Decompress(_)
            | Error::Eof(_)
    )
}

fn body_too_large(max_len: usize) -> Error {
    Error::Network(format!("response body exceeds {max_len} bytes"))
}

/// Read body up to `max_len`, paced.
pub(crate) async fn read_paced(
    mut response: Response,
    limiter: Option<&RateLimiter>,
    max_len: usize,
) -> Result<bytes::Bytes, Error> {
    let declared = response.content_length().unwrap_or(0);
    if declared > max_len as u64 {
        return Err(body_too_large(max_len));
    }
    let limiter = limiter.filter(|l| l.is_limited());
    let hint = (declared as usize).min(max_len).min(64 << 20);
    let mut body = Vec::with_capacity(hint);
    while let Some(piece) = response.chunk().await? {
        if body.len().saturating_add(piece.len()) > max_len {
            return Err(body_too_large(max_len));
        }
        if let Some(limiter) = limiter {
            limiter.acquire(piece.len() as u64).await;
        }
        body.extend_from_slice(&piece);
    }
    Ok(body.into())
}

/// HTTP client with stall protection.
fn web_client() -> Client {
    Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(64)
        .build()
        .unwrap_or_else(|_| Client::new())
}

impl InnerClient {
    pub fn new(connection: Arc<Connection>) -> Self {
        Self {
            connection,
            web_client: web_client(),
            servers: Vec::new(),
            strikes: Vec::new(),
            next: AtomicUsize::new(0),
            limiter: OnceLock::new(),
        }
    }

    /// Attach a rate limit once.
    pub fn set_limiter(&self, limiter: Arc<RateLimiter>) -> bool {
        self.limiter.set(limiter).is_ok()
    }

    /// The active limit, if any.
    pub fn limiter(&self) -> Option<&Arc<RateLimiter>> {
        self.limiter.get().filter(|l| l.is_limited())
    }

    /// Body bytes, paced by the limit.
    async fn read_body(&self, response: Response, max_len: usize) -> Result<bytes::Bytes, Error> {
        read_paced(response, self.limiter().map(|l| l.as_ref()), max_len).await
    }

    /// Replace the pool, in preference order.
    pub fn set_servers(&mut self, servers: Vec<CDNServer>) {
        self.strikes = servers.iter().map(|_| AtomicU32::new(0)).collect();
        self.servers = servers;
    }

    pub fn cell_id(&self) -> u32 {
        self.connection.cell_id()
    }

    /// Next healthy server, round-robin.
    fn pick_server(&self) -> Result<usize, Error> {
        let strikes: Vec<u32> = self.strikes.iter().map(|s| s.load(Ordering::Relaxed)).collect();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        pick_index(&strikes, start).ok_or(Error::Network("no available cdn servers".to_string()))
    }

    fn record(&self, index: usize, ok: bool) {
        let Some(strikes) = self.strikes.get(index) else {
            return;
        };
        if ok {
            // Recovered hosts earn their way back.
            let _ = strikes.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |s| s.checked_sub(1));
        } else {
            strikes.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// GET with retries across the pool.
    async fn get_with_failover(
        &self,
        command: &str,
        args: &str,
        manifest_request_code: Option<u64>,
        attempts: u32,
        max_body: usize,
    ) -> Result<bytes::Bytes, Error> {
        let mut last = Error::Network("no available cdn servers".to_string());
        for attempt in 0..attempts {
            let index = self.pick_server()?;
            let result = async {
                let response = self
                    .request_on(index, command, args, manifest_request_code)
                    .await?;
                let status = response.status();
                if !status.is_success() {
                    return Err(Error::HttpStatus(status));
                }
                self.read_body(response, max_body).await
            }
            .await;
            match result {
                Ok(bytes) => {
                    self.record(index, true);
                    return Ok(bytes);
                }
                Err(e) if retryable(&e) => {
                    self.record(index, false);
                    tracing::debug!(
                        "cdn {} attempt {}: {command}/{args}: {e}",
                        self.servers[index].host,
                        attempt + 1
                    );
                    last = e;
                    if attempt + 1 < attempts {
                        tokio::time::sleep(backoff(attempt)).await;
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Err(last)
    }

    pub async fn get_product_info(
        &self,
        app_ids: Vec<u32>,
    ) -> Result<CMsgClientPICSProductInfoResponse, Error> {
        let tokens: CMsgClientPICSAccessTokenResponse = self
            .connection
            .job(CMsgClientPICSAccessTokenRequest {
                appids: app_ids,
                ..Default::default()
            })
            .await?;

        let product_info: CMsgClientPICSProductInfoResponse = self
            .connection
            .job(CMsgClientPICSProductInfoRequest {
                apps: tokens
                    .app_access_tokens
                    .into_iter()
                    .map(|app_token| AppInfo {
                        appid: app_token.appid,
                        access_token: app_token.access_token,
                        ..Default::default()
                    })
                    .collect::<Vec<AppInfo>>(),
                meta_data_only: Some(false),
                ..Default::default()
            })
            .await?;
        Ok(product_info)
    }

    /// One request to server `index`.
    async fn request_on(
        &self,
        index: usize,
        command: &str,
        args: &str,
        manifest_request_code: Option<u64>,
    ) -> Result<Response, Error> {
        let server = &self.servers[index];
        let mut url = format!(
            "{}://{}:{}/{}/{}",
            if server.https { "https" } else { "http" },
            server.host,
            server.port,
            command,
            args
        );
        if let Some(manifest_request_code) = manifest_request_code {
            url.push('/');
            url.push_str(manifest_request_code.to_string().as_str());
        }

        let mut request = self.web_client.get(&url);

        if let Some(token) = &server.auth_token {
            url.push_str("?token=");
            url.push_str(token);
            request = self.web_client.get(&url).header("x-cdn-auth-token", token);
        }

        let response = request.send().await?;

        Ok(response)
    }

    /// Manifest bytes, with failover.
    pub async fn get_manifest_bytes(
        &self,
        depot_id: u32,
        manifest_id: u64,
        request_code: Option<u64>,
        version: usize,
    ) -> Result<bytes::Bytes, Error> {
        let args = format!("{depot_id}/manifest/{manifest_id}/{version}");
        self.get_with_failover(
            "depot",
            &args,
            request_code,
            CHUNK_ATTEMPTS,
            MAX_MANIFEST_BODY,
        )
        .await
    }

    /// Fetch one chunk, manifest-verified.
    pub async fn get_chunk(
        &self,
        _app_id: u32,
        depot_id: u32,
        depot_key: [u8; 32],
        chunk_id: String,
        expected_sha: &[u8],
        expected_len: u32,
    ) -> Result<Vec<u8>, Error> {
        // Reject before any fetch.
        if expected_len as usize > crate::utils::lzma::MAX_CHUNK_LEN {
            return Err(Error::Unexpected(format!(
                "chunk {chunk_id}: {expected_len} bytes exceeds ceiling"
            )));
        }
        let args = format!("{depot_id}/chunk/{chunk_id}");
        let mut last = Error::Network("no available cdn servers".to_string());
        for attempt in 0..CHUNK_ATTEMPTS {
            let bytes = self
                .get_with_failover("depot", &args, None, 1, MAX_CHUNK_BODY)
                .await;
            // Corrupt transfers count as failures.
            let result = match bytes {
                Ok(bytes) => {
                    let mut bytes = bytes.to_vec();
                    depot_chunk::decrypt_and_decompress(
                        &mut bytes[..],
                        depot_key,
                        expected_len as usize,
                    )
                    .await
                    .and_then(|data| check_chunk(&chunk_id, data, expected_sha, expected_len))
                }
                Err(e) => Err(e),
            };
            match result {
                Ok(data) => return Ok(data),
                Err(e) if retryable(&e) => {
                    last = e;
                    if attempt + 1 < CHUNK_ATTEMPTS {
                        tokio::time::sleep(backoff(attempt)).await;
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Err(last)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sha(data: &[u8]) -> Vec<u8> {
        use sha1::Digest;
        sha1::Sha1::digest(data).to_vec()
    }

    #[test]
    fn matching_chunk_passes() {
        let data = b"chunk bytes".to_vec();
        let hash = sha(&data);
        assert_eq!(check_chunk("c", data.clone(), &hash, 11).unwrap(), data);
    }

    #[test]
    fn substituted_chunk_is_rejected_and_retryable() {
        let err = check_chunk("c", b"other bytes".to_vec(), &sha(b"chunk bytes"), 11).unwrap_err();
        assert!(retryable(&err), "{err}");
    }

    #[test]
    fn wrong_length_chunk_is_rejected() {
        let data = b"chunk bytes!".to_vec();
        let hash = sha(&data);
        assert!(check_chunk("c", data, &hash, 11).is_err());
    }

    #[test]
    fn picks_round_robin_from_start() {
        assert_eq!(pick_index(&[0, 0, 0], 0), Some(0));
        assert_eq!(pick_index(&[0, 0, 0], 4), Some(1));
        assert_eq!(pick_index(&[], 3), None);
    }

    #[test]
    fn skips_benched_hosts() {
        let strikes = [BENCH_STRIKES, 0, BENCH_STRIKES];
        for start in 0..6 {
            assert_eq!(pick_index(&strikes, start), Some(1));
        }
    }

    #[test]
    fn all_benched_takes_least_failing() {
        let strikes = [BENCH_STRIKES + 2, BENCH_STRIKES, BENCH_STRIKES + 1];
        assert_eq!(pick_index(&strikes, 0), Some(1));
    }

    #[test]
    fn backoff_grows_and_caps() {
        assert_eq!(backoff(0), Duration::from_millis(200));
        assert_eq!(backoff(1), Duration::from_millis(400));
        assert_eq!(backoff(3), Duration::from_millis(1600));
        assert_eq!(backoff(9), Duration::from_millis(3000));
    }

    #[test]
    fn only_transfer_errors_retry() {
        assert!(retryable(&Error::Request("reset".into())));
        assert!(retryable(&Error::HttpStatus(reqwest::StatusCode::BAD_GATEWAY)));
        assert!(retryable(&Error::Decompress("bad zip".into())));
        // Disk errors never retry.
        let full = std::io::Error::from(std::io::ErrorKind::StorageFull);
        assert!(!retryable(&Error::Io(full)));
        assert!(!retryable(&Error::Unexpected("x".into())));
    }
}

#[cfg(test)]
mod pacing_tests {
    use super::*;
    use std::time::Instant;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Serve `len` bytes once.
    async fn serve(len: usize) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 1024];
            let _ = socket.read(&mut request).await;
            let head = format!("HTTP/1.1 200 OK\r\ncontent-length: {len}\r\n\r\n");
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(&vec![7u8; len]).await.unwrap();
        });
        format!("http://{addr}/")
    }

    /// Serve `len` bytes without length.
    async fn serve_unsized(len: usize) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 1024];
            let _ = socket.read(&mut request).await;
            let head = "HTTP/1.1 200 OK\r\nconnection: close\r\n\r\n";
            socket.write_all(head.as_bytes()).await.unwrap();
            let _ = socket.write_all(&vec![7u8; len]).await;
        });
        format!("http://{addr}/")
    }

    #[tokio::test]
    async fn declared_oversized_body_is_rejected() {
        let url = serve(5_000).await;
        let response = Client::new().get(url).send().await.unwrap();
        let err = read_paced(response, None, 4_096).await.unwrap_err();
        assert!(matches!(err, Error::Network(_)), "{err}");
    }

    #[tokio::test]
    async fn streamed_oversized_body_is_rejected() {
        let url = serve_unsized(5_000).await;
        let response = Client::new().get(url).send().await.unwrap();
        let err = read_paced(response, None, 4_096).await.unwrap_err();
        assert!(matches!(err, Error::Network(_)), "{err}");
    }

    #[tokio::test]
    async fn body_at_limit_is_kept() {
        let url = serve(4_096).await;
        let response = Client::new().get(url).send().await.unwrap();
        assert_eq!(
            read_paced(response, None, 4_096).await.unwrap().len(),
            4_096
        );
    }

    #[tokio::test]
    async fn limited_body_takes_its_time() {
        let url = serve(40_000).await;
        let response = Client::new().get(url).send().await.unwrap();
        let limiter = RateLimiter::new(100_000);
        let start = Instant::now();
        let body = read_paced(response, Some(&limiter), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.len(), 40_000);
        assert!(start.elapsed() >= Duration::from_millis(350), "{:?}", start.elapsed());
    }

    #[tokio::test]
    async fn unlimited_body_is_read_whole() {
        let url = serve(40_000).await;
        let response = Client::new().get(url).send().await.unwrap();
        let body = read_paced(response, Some(&RateLimiter::new(0)), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.len(), 40_000);
    }
}
