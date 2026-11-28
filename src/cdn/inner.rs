// Modified by Aurelia; see README.
use reqwest::{Client, Response};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use steam_vent::{Connection, ConnectionTrait};
use steam_vent_proto::steammessages_clientserver_appinfo::{
    CMsgClientPICSAccessTokenRequest, CMsgClientPICSAccessTokenResponse,
    CMsgClientPICSProductInfoRequest, CMsgClientPICSProductInfoResponse,
    cmsg_client_picsproduct_info_request::AppInfo,
};

use crate::{Error, web_api::content_service::CDNServer};

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
#[cfg(test)]
pub(crate) fn pick_index(strikes: &[u32], start: usize) -> Option<usize> {
    pick_index_excluding(strikes, start, &[])
}

/// Healthy index, never a `refused` one.
pub(crate) fn pick_index_excluding(
    strikes: &[u32],
    start: usize,
    refused: &[usize],
) -> Option<usize> {
    let n = strikes.len();
    let open = |i: &usize| !refused.contains(i);
    (0..n)
        .map(|i| (start + i) % n)
        .filter(open)
        .find(|&i| strikes[i] < BENCH_STRIKES)
        // All benched: least-failing one.
        .or_else(|| (0..n).filter(open).min_by_key(|&i| strikes[i]))
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
        return Err(Error::Decompress(format!(
            "chunk {chunk_id}: SHA-1 mismatch"
        )));
    }
    Ok(data)
}

/// Whether another try may succeed.
pub(crate) fn retryable(err: &Error) -> bool {
    match err {
        Error::HttpStatus(status) => server_busy(*status) || *status == 408,
        Error::Request(_) | Error::Network(_) | Error::Decompress(_) | Error::Eof(_) => true,
        _ => false,
    }
}

/// Other 4xx: another host may serve it.
pub(crate) fn host_refused(err: &Error) -> bool {
    matches!(err, Error::HttpStatus(s) if s.is_client_error() && !retryable(err))
}

/// 5xx and 429: wait, then retry.
fn server_busy(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS
}

/// Longest `Retry-After` honoured.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);

/// Delay for busy servers: 1/2/4/8 s.
pub(crate) fn busy_backoff(attempt: u32) -> Duration {
    Duration::from_secs(1u64 << attempt.min(3))
}

/// Delay before the next try.
pub(crate) fn retry_delay(err: &Error, attempt: u32, retry_after: Option<Duration>) -> Duration {
    match err {
        Error::HttpStatus(status) if server_busy(*status) => {
            busy_backoff(attempt).max(retry_after.unwrap_or_default().min(MAX_RETRY_AFTER))
        }
        _ => backoff(attempt),
    }
}

/// A failed try, with the server's hint.
#[derive(Debug)]
pub(crate) struct Failure {
    pub err: Error,
    pub retry_after: Option<Duration>,
    /// Host index that answered.
    pub host: Option<usize>,
}

impl From<Error> for Failure {
    fn from(err: Error) -> Self {
        Self {
            err,
            retry_after: None,
            host: None,
        }
    }
}

/// Seconds form of `Retry-After`.
fn retry_after(response: &Response) -> Option<Duration> {
    let value = response.headers().get(reqwest::header::RETRY_AFTER)?;
    value
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

/// Status check, then the paced body.
pub(crate) async fn response_outcome(
    response: Response,
    limiter: Option<&RateLimiter>,
    max_len: usize,
) -> Result<bytes::Bytes, Failure> {
    let status = response.status();
    if !status.is_success() {
        return Err(Failure {
            retry_after: retry_after(&response),
            err: Error::HttpStatus(status),
            host: None,
        });
    }
    Ok(read_paced(response, limiter, max_len).await?)
}

/// Run `fetch` up to `attempts` times.
///
/// Busy or broken replies back off.
/// A 4xx asks an unasked host.
/// Fails once every host refused.
pub(crate) async fn retry_with<T, F, Fut, S, SFut>(
    attempts: u32,
    host_count: usize,
    mut fetch: F,
    mut sleep: S,
) -> Result<T, Error>
where
    F: FnMut(u32, Vec<usize>) -> Fut,
    Fut: std::future::Future<Output = Result<T, Failure>>,
    S: FnMut(Duration) -> SFut,
    SFut: std::future::Future<Output = ()>,
{
    let mut last = Error::Network("no available cdn servers".to_string());
    let mut refused: Vec<usize> = Vec::new();
    let mut refusals = 0;
    for attempt in 0..attempts {
        match fetch(attempt, refused.clone()).await {
            Ok(value) => return Ok(value),
            Err(Failure {
                err, retry_after, ..
            }) if retryable(&err) => {
                let delay = retry_delay(&err, attempt, retry_after);
                last = err;
                if attempt + 1 < attempts {
                    sleep(delay).await;
                }
            }
            Err(Failure { err, host, .. }) if host_refused(&err) => {
                match host {
                    Some(i) if !refused.contains(&i) => refused.push(i),
                    Some(_) => {}
                    // Unknown host: count the refusal.
                    None => refusals += 1,
                }
                if refused.len() + refusals >= host_count.max(1) {
                    return Err(err);
                }
                last = err;
            }
            Err(Failure { err, .. }) => return Err(err),
        }
    }
    Err(last)
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
        .http1_only()
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
        self.pick_server_excluding(&[])
    }

    /// Next healthy server not in `refused`.
    fn pick_server_excluding(&self, refused: &[usize]) -> Result<usize, Error> {
        let strikes: Vec<u32> = self
            .strikes
            .iter()
            .map(|s| s.load(Ordering::Relaxed))
            .collect();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        pick_index_excluding(&strikes, start, refused)
            .ok_or(Error::Network("no available cdn servers".to_string()))
    }

    fn record(&self, index: usize, ok: bool) {
        let Some(strikes) = self.strikes.get(index) else {
            return;
        };
        if ok {
            // Recovered hosts earn their way back.
            let _ = strikes.try_update(Ordering::Relaxed, Ordering::Relaxed, |s| s.checked_sub(1));
        } else {
            strikes.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// One GET on a healthy, unrefused server.
    async fn fetch_once(
        &self,
        command: &str,
        args: &str,
        manifest_request_code: Option<u64>,
        max_body: usize,
        refused: &[usize],
    ) -> Result<bytes::Bytes, Failure> {
        let index = self.pick_server_excluding(refused)?;
        let result = async {
            let response = self
                .request_on(index, command, args, manifest_request_code)
                .await?;
            response_outcome(response, self.limiter().map(|l| l.as_ref()), max_body).await
        }
        .await;
        match &result {
            Ok(_) => self.record(index, true),
            Err(failure) if retryable(&failure.err) || host_refused(&failure.err) => {
                self.record(index, false);
                tracing::debug!(
                    "cdn {}: {command}/{args}: {}",
                    self.servers[index].host,
                    failure.err
                );
            }
            Err(_) => {}
        }
        result.map_err(|failure| Failure {
            host: Some(index),
            ..failure
        })
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
        self.pick_server()?;
        retry_with(
            attempts,
            self.servers.len(),
            |_, refused| async move {
                self.fetch_once(command, args, manifest_request_code, max_body, &refused)
                    .await
            },
            tokio::time::sleep,
        )
        .await
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
        let args = &format!("{depot_id}/chunk/{chunk_id}");
        let chunk_id = &chunk_id;
        self.pick_server()?;
        retry_with(
            CHUNK_ATTEMPTS,
            self.servers.len(),
            |_, refused| async move {
                let bytes = self
                    .fetch_once("depot", args, None, MAX_CHUNK_BODY, &refused)
                    .await?;
                // Corrupt transfers count as failures.
                let mut bytes = bytes.to_vec();
                let data = depot_chunk::decrypt_and_decompress(
                    &mut bytes[..],
                    depot_key,
                    expected_len as usize,
                )
                .await?;
                Ok(check_chunk(chunk_id, data, expected_sha, expected_len)?)
            },
            tokio::time::sleep,
        )
        .await
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
    fn never_picks_a_refused_host() {
        for start in 0..6 {
            let pick = pick_index_excluding(&[0, 0, 0], start, &[1]);
            assert_ne!(pick, Some(1), "start {start}");
            assert!(pick.is_some());
        }
    }

    #[test]
    fn refused_beats_strikes() {
        // Benched but unrefused wins.
        let strikes = [0, BENCH_STRIKES + 5, 0];
        assert_eq!(pick_index_excluding(&strikes, 0, &[0, 2]), Some(1));
        let strikes = [BENCH_STRIKES, BENCH_STRIKES + 1, BENCH_STRIKES + 2];
        assert_eq!(pick_index_excluding(&strikes, 0, &[0]), Some(1));
    }

    #[test]
    fn all_refused_picks_nothing() {
        for start in 0..4 {
            assert_eq!(pick_index_excluding(&[0, 0, 0], start, &[0, 1, 2]), None);
        }
        assert_eq!(pick_index_excluding(&[], 0, &[]), None);
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
        assert!(retryable(&Error::HttpStatus(
            reqwest::StatusCode::BAD_GATEWAY
        )));
        assert!(retryable(&Error::Decompress("bad zip".into())));
        assert!(retryable(&Error::HttpStatus(
            reqwest::StatusCode::TOO_MANY_REQUESTS
        )));
        // Other 4xx: next host, no wait.
        assert!(!retryable(&Error::HttpStatus(
            reqwest::StatusCode::NOT_FOUND
        )));
        assert!(!retryable(&Error::HttpStatus(
            reqwest::StatusCode::FORBIDDEN
        )));
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
        assert!(
            start.elapsed() >= Duration::from_millis(350),
            "{:?}",
            start.elapsed()
        );
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

#[cfg(test)]
mod retry_tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Answer each request with the next reply.
    async fn serve_sequence(replies: Vec<&'static str>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for reply in replies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 1024];
                let _ = socket.read(&mut request).await;
                socket.write_all(reply.as_bytes()).await.unwrap();
            }
        });
        format!("http://{addr}/")
    }

    const BUSY: &str =
        "HTTP/1.1 503 Service Unavailable\r\nconnection: close\r\ncontent-length: 0\r\n\r\n";
    const BUSY_HINT: &str = "HTTP/1.1 503 Service Unavailable\r\nretry-after: 5\r\nconnection: close\r\ncontent-length: 0\r\n\r\n";
    const MISSING: &str =
        "HTTP/1.1 404 Not Found\r\nconnection: close\r\ncontent-length: 0\r\n\r\n";
    const OK: &str = "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-length: 2\r\n\r\nok";

    /// Retry against `url`, recording sleeps.
    async fn run(
        url: &str,
        attempts: u32,
        hosts: usize,
    ) -> (Result<bytes::Bytes, Error>, Vec<Duration>, u32) {
        let client = Client::new();
        let slept = Mutex::new(Vec::new());
        let tries = Mutex::new(0u32);
        let result = retry_with(
            attempts,
            hosts,
            |_, _| {
                *tries.lock().unwrap() += 1;
                let client = client.clone();
                let url = url.to_string();
                async move {
                    let response = client.get(url).send().await.map_err(Error::from)?;
                    response_outcome(response, None, 1024).await
                }
            },
            |delay| {
                slept.lock().unwrap().push(delay);
                async {}
            },
        )
        .await;
        let tries = *tries.lock().unwrap();
        (result, slept.into_inner().unwrap(), tries)
    }

    #[tokio::test]
    async fn busy_then_ok_succeeds_with_backoff() {
        let url = serve_sequence(vec![BUSY, BUSY, OK]).await;
        let (result, slept, tries) = run(&url, 5, 4).await;
        assert_eq!(&result.unwrap()[..], b"ok");
        assert_eq!(tries, 3);
        assert_eq!(slept, vec![Duration::from_secs(1), Duration::from_secs(2)]);
    }

    #[tokio::test]
    async fn not_found_on_the_only_host_fails_fast() {
        let url = serve_sequence(vec![MISSING, OK]).await;
        let (result, slept, tries) = run(&url, 5, 1).await;
        assert!(matches!(result, Err(Error::HttpStatus(s)) if s == 404));
        assert_eq!(tries, 1);
        assert!(slept.is_empty());
    }

    #[tokio::test]
    async fn gives_up_after_the_attempts() {
        let url = serve_sequence(vec![BUSY, BUSY, BUSY]).await;
        let (result, slept, tries) = run(&url, 3, 4).await;
        assert!(matches!(result, Err(Error::HttpStatus(s)) if s == 503));
        assert_eq!(tries, 3);
        // No sleep after the last try.
        assert_eq!(slept.len(), 2);
    }

    #[tokio::test]
    async fn retry_after_is_honoured_and_capped() {
        let url = serve_sequence(vec![BUSY_HINT, OK]).await;
        let (result, slept, _) = run(&url, 5, 4).await;
        assert!(result.is_ok());
        assert_eq!(slept, vec![Duration::from_secs(5)]);
        let busy = Error::HttpStatus(reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            retry_delay(&busy, 0, Some(Duration::from_secs(600))),
            MAX_RETRY_AFTER
        );
    }

    #[tokio::test]
    async fn refusals_move_to_the_next_host_without_waiting() {
        let url = serve_sequence(vec![MISSING, MISSING, OK]).await;
        let (result, slept, tries) = run(&url, 5, 3).await;
        assert_eq!(&result.unwrap()[..], b"ok");
        assert_eq!(tries, 3);
        assert!(slept.is_empty());
    }

    #[tokio::test]
    async fn fails_once_every_host_refused() {
        let url = serve_sequence(vec![MISSING, MISSING, MISSING, OK]).await;
        let (result, slept, tries) = run(&url, 5, 3).await;
        assert!(matches!(result, Err(Error::HttpStatus(s)) if s == 404));
        assert_eq!(tries, 3);
        assert!(slept.is_empty());
    }

    /// Per-host replies, cursor pinned at 0.
    async fn run_hosts(
        replies: &[&[u16]],
        attempts: u32,
    ) -> (Result<u16, Error>, Vec<usize>, Vec<Duration>) {
        let served = Mutex::new(vec![0usize; replies.len()]);
        let asked = Mutex::new(Vec::new());
        let slept = Mutex::new(Vec::new());
        let strikes = Mutex::new(vec![0u32; replies.len()]);
        let result = retry_with(
            attempts,
            replies.len(),
            |_, refused| {
                let strikes_now = strikes.lock().unwrap().clone();
                let host = pick_index_excluding(&strikes_now, 0, &refused);
                let reply = host.map(|i| {
                    asked.lock().unwrap().push(i);
                    let mut served = served.lock().unwrap();
                    let n = served[i];
                    served[i] += 1;
                    replies[i][n.min(replies[i].len() - 1)]
                });
                if let (Some(i), Some(code)) = (host, reply)
                    && code != 200
                {
                    strikes.lock().unwrap()[i] += 1;
                }
                async move {
                    match reply {
                        None => Err(Error::Network("none".into()).into()),
                        Some(200) => Ok(200),
                        Some(code) => Err(Failure {
                            err: Error::HttpStatus(reqwest::StatusCode::from_u16(code).unwrap()),
                            retry_after: None,
                            host,
                        }),
                    }
                }
            },
            |delay| {
                slept.lock().unwrap().push(delay);
                async {}
            },
        )
        .await;
        (
            result,
            asked.into_inner().unwrap(),
            slept.into_inner().unwrap(),
        )
    }

    #[tokio::test]
    async fn each_refusing_host_is_asked_once() {
        let (result, asked, slept) = run_hosts(&[&[404, 200], &[404, 200], &[200]], 5).await;
        assert_eq!(result.unwrap(), 200);
        // Host 0 refused; never asked again.
        assert_eq!(asked, vec![0, 1, 2]);
        assert!(slept.is_empty());
    }

    #[tokio::test]
    async fn refusing_host_is_not_asked_twice() {
        let (result, asked, _) = run_hosts(&[&[404, 200], &[200]], 5).await;
        assert_eq!(result.unwrap(), 200);
        assert_eq!(asked, vec![0, 1]);
    }

    #[tokio::test]
    async fn fails_after_every_distinct_host_refused() {
        let (result, asked, slept) = run_hosts(&[&[404], &[403], &[404]], 5).await;
        assert!(matches!(result, Err(Error::HttpStatus(s)) if s == 404));
        assert_eq!(asked, vec![0, 1, 2]);
        assert!(slept.is_empty());
    }

    #[tokio::test]
    async fn busy_host_stays_eligible() {
        let (result, asked, slept) = run_hosts(&[&[503, 200], &[200]], 5).await;
        assert_eq!(result.unwrap(), 200);
        // 503 is no refusal.
        assert_eq!(asked, vec![0, 0]);
        assert_eq!(slept, vec![Duration::from_secs(1)]);
    }

    #[test]
    fn busy_backoff_doubles_to_eight_seconds() {
        let secs: Vec<u64> = (0..6).map(|a| busy_backoff(a).as_secs()).collect();
        assert_eq!(secs, vec![1, 2, 4, 8, 8, 8]);
    }

    #[test]
    fn corrupt_transfers_keep_the_short_backoff() {
        let corrupt = Error::Decompress("bad zip".into());
        assert_eq!(retry_delay(&corrupt, 0, None), backoff(0));
    }
}
