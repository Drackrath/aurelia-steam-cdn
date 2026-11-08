//! Shared download rate limit.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Bytes per second per stream.
const PER_STREAM: u64 = 256 * 1024;

/// Token bucket shared across downloads.
///
/// A rate of 0 means unlimited. The rate can change while downloads run.
#[derive(Debug)]
pub struct RateLimiter {
    rate: AtomicU64,
    bucket: Mutex<Bucket>,
}

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last: Instant,
    rate: u64,
}

impl RateLimiter {
    /// Limit to `bytes_per_sec`; 0 disables.
    pub fn new(bytes_per_sec: u64) -> Self {
        Self {
            rate: AtomicU64::new(bytes_per_sec),
            bucket: Mutex::new(Bucket {
                tokens: 0.0,
                last: Instant::now(),
                rate: bytes_per_sec,
            }),
        }
    }

    /// Change the limit; 0 disables.
    pub fn set_rate(&self, bytes_per_sec: u64) {
        self.rate.store(bytes_per_sec, Ordering::Relaxed);
    }

    /// Current limit in bytes per second.
    pub fn rate(&self) -> u64 {
        self.rate.load(Ordering::Relaxed)
    }

    /// Whether a limit is active.
    pub fn is_limited(&self) -> bool {
        self.rate() > 0
    }

    /// Wait until `bytes` fit the limit.
    pub async fn acquire(&self, bytes: u64) {
        let wait = self.reserve(bytes, Instant::now());
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }

    /// Book `bytes`; returns the delay owed.
    fn reserve(&self, bytes: u64, now: Instant) -> Duration {
        let rate = self.rate();
        if rate == 0 || bytes == 0 {
            return Duration::ZERO;
        }
        let mut bucket = self.bucket.lock().unwrap_or_else(|e| e.into_inner());
        if bucket.rate != rate {
            // New limit starts fresh.
            bucket.rate = rate;
            bucket.tokens = 0.0;
            bucket.last = now;
        }
        let elapsed = now.saturating_duration_since(bucket.last).as_secs_f64();
        let burst = rate as f64;
        bucket.tokens = (bucket.tokens + elapsed * rate as f64).min(burst);
        bucket.last = now;
        bucket.tokens -= bytes as f64;
        if bucket.tokens >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-bucket.tokens / rate as f64)
        }
    }

    /// Parallel streams suited to the limit.
    pub fn streams(&self, max: usize) -> usize {
        match self.rate() {
            0 => max,
            rate => ((rate / PER_STREAM) as usize).clamp(1, max.max(1)),
        }
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(d: Duration) -> f64 {
        d.as_secs_f64()
    }

    #[test]
    fn unlimited_never_waits() {
        let limiter = RateLimiter::new(0);
        assert!(limiter.reserve(u64::MAX, Instant::now()).is_zero());
        assert!(!limiter.is_limited());
    }

    #[test]
    fn debt_accumulates_across_callers() {
        let limiter = RateLimiter::new(1000);
        let start = Instant::now();
        assert!((secs(limiter.reserve(1000, start)) - 1.0).abs() < 1e-6);
        assert!((secs(limiter.reserve(500, start)) - 1.5).abs() < 1e-6);
    }

    #[test]
    fn idle_time_refills_up_to_one_second() {
        let limiter = RateLimiter::new(1000);
        let start = Instant::now();
        limiter.reserve(1, start);
        let later = start + Duration::from_secs(10);
        assert!(limiter.reserve(900, later).is_zero());
        assert!((secs(limiter.reserve(1000, later)) - 0.9).abs() < 1e-3);
    }

    #[test]
    fn rate_change_resets_bucket() {
        let limiter = RateLimiter::new(1000);
        let start = Instant::now();
        limiter.reserve(5000, start);
        limiter.set_rate(2000);
        assert!((secs(limiter.reserve(2000, start)) - 1.0).abs() < 1e-6);
        limiter.set_rate(0);
        assert!(limiter.reserve(1 << 30, start).is_zero());
    }

    #[test]
    fn streams_follow_the_rate() {
        assert_eq!(RateLimiter::new(0).streams(16), 16);
        assert_eq!(RateLimiter::new(1).streams(16), 1);
        assert_eq!(RateLimiter::new(PER_STREAM * 4).streams(16), 4);
        assert_eq!(RateLimiter::new(PER_STREAM * 100).streams(16), 16);
    }
}
