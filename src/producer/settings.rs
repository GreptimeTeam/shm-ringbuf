use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_GRPC_SOCK_PATH: &str = "/tmp/grpc.sock";
const DEFAULT_FDPASS_SOCK_PATH: &str = "/tmp/fdpass.sock";
const DEFAULT_RINGBUF_LEN: usize = 1024 * 1024;
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_RECONNECT_INTERVAL: Duration = Duration::from_secs(3);
const DEFAULT_EXPIRED_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_SUBSCRIPTION_TTL: Duration = Duration::from_secs(3);

#[derive(Debug, Clone)]
pub struct ProducerSettings {
    pub(super) grpc_sock_path: PathBuf,
    pub(super) fdpass_sock_path: PathBuf,
    pub(super) ringbuf_len: usize,
    pub(super) heartbeat_interval: Duration,
    pub(super) enable_result_fetch: bool,
    pub(super) reconnect_interval: Duration,
    pub(super) expired_check_interval: Duration,
    pub(super) subscription_ttl: Duration,
    pub(super) enable_checksum: bool,
    #[cfg(not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "freebsd"
    )))]
    pub(super) backed_file_path: PathBuf,
}

#[derive(Default)]
pub struct ProducerSettingsBuilder {
    grpc_sock_path: Option<PathBuf>,
    fdpass_sock_path: Option<PathBuf>,
    ringbuf_len: Option<usize>,
    heartbeat_interval: Option<Duration>,
    enable_result_fetch: Option<bool>,
    reconnect_interval: Option<Duration>,
    expired_check_interval: Option<Duration>,
    subscription_ttl: Option<Duration>,
    enable_checksum: Option<bool>,
    #[cfg(not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "freebsd"
    )))]
    pub(super) backed_file_path: Option<PathBuf>,
}

impl ProducerSettingsBuilder {
    pub fn new() -> Self {
        ProducerSettingsBuilder::default()
    }

    /// Set the path of the unix socket for gRPC communication.
    pub fn grpc_sock_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.grpc_sock_path = Some(path.into());
        self
    }

    /// Set the path of the unix socket for passing file descriptor and other
    /// information.
    pub fn fdpass_sock_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.fdpass_sock_path = Some(path.into());
        self
    }

    /// Set the ringbuf length. The actual length of the ringbuf may be larger
    /// than the setting.
    pub fn ringbuf_len(mut self, len: usize) -> Self {
        self.ringbuf_len = Some(len);
        self
    }

    /// Set the heartbeat interval.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = Some(interval);
        self
    }

    /// Enable fetching the result of consumer processing data.
    pub fn enable_result_fetch(mut self, enable: bool) -> Self {
        self.enable_result_fetch = Some(enable);
        self
    }

    /// Set the interval for retrying to fetch the result.
    pub fn reconnect_interval(mut self, interval: Duration) -> Self {
        self.reconnect_interval = Some(interval);
        self
    }

    /// Set the interval for checking the expired result fetch subscriptions.
    pub fn expired_check_interval(mut self, interval: Duration) -> Self {
        self.expired_check_interval = Some(interval);
        self
    }

    /// Set the ttl(time to live) for the subscription.
    pub fn subscription_ttl(mut self, timeout: Duration) -> Self {
        self.subscription_ttl = Some(timeout);
        self
    }

    /// Enable verify data consistency by checksum.
    pub fn enable_checksum(mut self, enable: bool) -> Self {
        self.enable_checksum = Some(enable);
        self
    }

    #[cfg(not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "freebsd"
    )))]
    /// Set the path of the backed file.
    pub fn backed_file_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.backed_file_path = Some(path.into());
        self
    }

    pub fn build(self) -> ProducerSettings {
        let grpc_sock_path = self
            .grpc_sock_path
            .unwrap_or_else(|| PathBuf::from(DEFAULT_GRPC_SOCK_PATH));

        let fdpass_sock_path = self
            .fdpass_sock_path
            .unwrap_or_else(|| PathBuf::from(DEFAULT_FDPASS_SOCK_PATH));

        let ringbuf_len = self.ringbuf_len.unwrap_or(DEFAULT_RINGBUF_LEN);

        let heartbeat_interval = self
            .heartbeat_interval
            .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL);

        let enable_result_fetch = self.enable_result_fetch.unwrap_or(true);

        let reconnect_interval = self
            .reconnect_interval
            .unwrap_or(DEFAULT_RECONNECT_INTERVAL);

        let expired_check_interval = self
            .expired_check_interval
            .unwrap_or(DEFAULT_EXPIRED_CHECK_INTERVAL);

        let subscription_ttl =
            self.subscription_ttl.unwrap_or(DEFAULT_SUBSCRIPTION_TTL);

        let enable_checksum = self.enable_checksum.unwrap_or(false);

        #[cfg(not(any(
            target_os = "linux",
            target_os = "android",
            target_os = "freebsd"
        )))]
        let backed_file_path = self
            .backed_file_path
            .unwrap_or_else(|| PathBuf::from("/tmp/shm.sock"));

        ProducerSettings {
            grpc_sock_path,
            fdpass_sock_path,
            ringbuf_len,
            heartbeat_interval,
            enable_result_fetch,
            reconnect_interval,
            expired_check_interval,
            subscription_ttl,
            enable_checksum,
            #[cfg(not(any(
                target_os = "linux",
                target_os = "android",
                target_os = "freebsd"
            )))]
            backed_file_path,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::producer::settings::DEFAULT_FDPASS_SOCK_PATH;
    use crate::producer::settings::DEFAULT_GRPC_SOCK_PATH;
    use crate::producer::settings::DEFAULT_HEARTBEAT_INTERVAL;
    use crate::producer::settings::DEFAULT_RINGBUF_LEN;

    #[test]
    fn test_default_settings() {
        let settings = super::ProducerSettingsBuilder::new().build();

        assert_eq!(
            settings.grpc_sock_path,
            PathBuf::from(DEFAULT_GRPC_SOCK_PATH)
        );
        assert_eq!(
            settings.fdpass_sock_path,
            PathBuf::from(DEFAULT_FDPASS_SOCK_PATH)
        );
        assert_eq!(settings.ringbuf_len, DEFAULT_RINGBUF_LEN);
        assert_eq!(settings.heartbeat_interval, DEFAULT_HEARTBEAT_INTERVAL);
    }

    #[test]
    fn test_settings() {
        let settings = super::ProducerSettingsBuilder::new()
            .grpc_sock_path("/tmp/grpc.sock")
            .fdpass_sock_path("/tmp/fdpass.sock")
            .ringbuf_len(1024 * 1024)
            .heartbeat_interval(std::time::Duration::from_secs(5))
            .build();

        assert_eq!(settings.grpc_sock_path, PathBuf::from("/tmp/grpc.sock"));
        assert_eq!(
            settings.fdpass_sock_path,
            PathBuf::from("/tmp/fdpass.sock")
        );
        assert_eq!(settings.ringbuf_len, 1024 * 1024);
        assert_eq!(
            settings.heartbeat_interval,
            std::time::Duration::from_secs(5)
        );
    }
}
