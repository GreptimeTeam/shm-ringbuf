use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use interprocess::local_socket::tokio::RecvHalf;
use snafu::{ensure, ResultExt};
use tokio::io::AsyncReadExt;
use tokio::sync::oneshot::channel;
use tokio::sync::oneshot::Receiver;
use tokio::sync::oneshot::Sender;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::error;
use crate::error::Result;
use crate::error::{DataProcessResult, IoSnafu};
use crate::grpc::proto::ResultSet;

pub type RequestId = u32;

pub struct ResultSetReceiver {
    rx: RecvHalf,
}

impl ResultSetReceiver {
    pub fn new(rx: RecvHalf) -> Self {
        Self { rx }
    }

    async fn recv(&mut self) -> Result<ResultSet> {
        let mut msg_len = [0u8; size_of::<usize>()];
        self.rx.read_exact(&mut msg_len).await.context(IoSnafu)?;
        let msg_len = usize::from_be_bytes(msg_len);

        let mut msg = vec![0; msg_len];
        self.rx.read_exact(&mut msg).await.context(IoSnafu)?;
        let result_set = bincode::deserialize::<ResultSet>(&msg)
            .unwrap_or_else(|e| panic!("todo: {e:?}"));
        Ok(result_set)
    }
}

/// The [`ResultFetcher`] has two abilities:
/// 1. Fetch the results of data processing on the consumer side.
/// 2. Send the fetched results to the corresponding subscribed producer.
pub struct ResultFetcher {
    inner: Arc<Inner>,
}

#[derive(Default)]
pub struct ResultFetcherMetrics {
    pub requests: AtomicU64,
    pub total_wait_micros: AtomicU64,
    pub remove_subscription_micros: AtomicU64,
    pub send_result_micros: AtomicU64,
}

impl ResultFetcherMetrics {
    fn record(
        &self,
        wait_duration: Duration,
        remove_subscription_duration: Duration,
        send_result_duration: Duration,
    ) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.total_wait_micros
            .fetch_add(wait_duration.as_micros() as u64, Ordering::Relaxed);
        self.remove_subscription_micros.fetch_add(
            remove_subscription_duration.as_micros() as u64,
            Ordering::Relaxed,
        );
        self.send_result_micros.fetch_add(
            send_result_duration.as_micros() as u64,
            Ordering::Relaxed,
        );
    }
}

struct Inner {
    subscriptions: DashMap<RequestId, (Sender<DataProcessResult>, Instant)>,
    subscription_ttl: Duration,
    expirations: RwLock<VecDeque<(RequestId, Instant)>>,
    metrics: ResultFetcherMetrics,
}

impl ResultFetcher {
    pub async fn new(
        result_set_receiver: ResultSetReceiver,
        // The interval for checking the expired result fetch subscriptions.
        expired_check_interval: Duration,
        subscription_ttl: Duration,
        cancel: CancellationToken,
    ) -> ResultFetcher {
        let subscriptions = DashMap::new();
        let expirations = RwLock::new(VecDeque::new());

        let inner = Inner {
            subscriptions,
            subscription_ttl,
            expirations,
            metrics: ResultFetcherMetrics::default(),
        };
        let inner = Arc::new(inner);

        let fetcher = ResultFetcher {
            inner: inner.clone(),
        };

        // Start a task to check the expired result fetch subscriptions periodically.
        let inner_c = inner.clone();
        tokio::spawn(async move {
            loop {
                if cancel.is_cancelled() {
                    break;
                }
                sleep(expired_check_interval).await;
                {
                    let mut expirations = inner_c.expirations.write().unwrap();
                    clean_expired_subscriptions(
                        &inner_c.subscriptions,
                        &mut expirations,
                    );
                }
            }
        });

        // Start a task to fetch the result stream.
        tokio::spawn({
            let mut result_set_receiver = result_set_receiver;
            async move {
                loop {
                    let result_set = result_set_receiver
                        .recv()
                        .await
                        .unwrap_or_else(|e| panic!("todo: {e:?}"));
                    fetcher.handle_result(result_set).await;
                }
            }
        });

        ResultFetcher { inner }
    }

    /// Subscribe to the result set corresponding to the request id. If the result
    /// fetcher is not ready, an error will be returned.
    pub fn subscribe(
        &self,
        request_id: u32,
    ) -> Result<Receiver<DataProcessResult>> {
        ensure!(self.is_normal(), error::ResultFetchNotReadySnafu);

        let (tx, rx) = channel();
        self.inner
            .subscriptions
            .insert(request_id, (tx, Instant::now()));
        let expired_at = Instant::now() + self.inner.subscription_ttl;
        self.inner
            .expirations
            .write()
            .unwrap()
            .push_back((request_id, expired_at));
        Ok(rx)
    }

    /// Check if the result fetcher is working normally.
    pub fn is_normal(&self) -> bool {
        // todo
        true
    }

    async fn handle_result(&self, result: ResultSet) {
        for result in result.results {
            let now = Instant::now();
            let subscription = self.inner.subscriptions.remove(&result.id);
            let remove_subscription_elapsed = now.elapsed();

            if let Some((_, (sender, start))) = subscription {
                let result = DataProcessResult {
                    status_code: result.status_code,
                    message: result.message,
                };
                let now = Instant::now();
                let _ = sender.send(result);
                let send_result_elapsed = now.elapsed();

                let wait_elapsed = start.elapsed();
                self.inner.metrics.record(
                    wait_elapsed,
                    remove_subscription_elapsed,
                    send_result_elapsed,
                );
            }
        }
    }

    pub fn metrics(&self) -> &ResultFetcherMetrics {
        &self.inner.metrics
    }
}

fn clean_expired_subscriptions(
    subscriptions: &DashMap<RequestId, (Sender<DataProcessResult>, Instant)>,
    expirations: &mut VecDeque<(RequestId, Instant)>,
) {
    let now = Instant::now();

    while let Some((req_id, expiration)) = expirations.front() {
        let req_id = *req_id;

        if *expiration > now {
            break;
        }

        expirations.pop_front();

        debug!("subscription expired, req id: {}", req_id);

        if let Some((_, sender)) = subscriptions.remove(&req_id) {
            let _ = sender.0.send(DataProcessResult {
                status_code: error::TIMEOUT,
                message: "subscription expired".to_string(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::time::{Duration, Instant};

    use dashmap::DashMap;

    #[tokio::test]
    async fn test_clean_expired_subscriptions() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let subscriptions = DashMap::new();
        subscriptions.insert(1, (tx, Instant::now()));

        let mut expirations = VecDeque::new();
        expirations
            .push_back((1, std::time::Instant::now() - Duration::from_secs(1)));

        super::clean_expired_subscriptions(&subscriptions, &mut expirations);

        assert!(subscriptions.is_empty());
        assert!(expirations.is_empty());

        let ret = rx.await.unwrap();
        assert_eq!(ret.status_code, super::error::TIMEOUT);
    }
}
