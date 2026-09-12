//! Filesystem attribute probing and error classification.

/// Whether a file is an un-hydrated (online-only) cloud placeholder.
///
/// `RECALL_ON_DATA_ACCESS` is the bit OneDrive Files-On-Demand sets on a placeholder and clears once the file is hydrated.
/// `RECALL_ON_OPEN` covers providers that hydrate on open rather than on read.
/// `OFFLINE` is the legacy HSM/archive bit.
///
/// The bit is binary, so a partially hydrated file still reports `true`.
#[cfg(windows)]
pub fn is_dehydrated(md: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;

    const FILE_ATTRIBUTE_OFFLINE: u32 = 0x0000_1000;
    const FILE_ATTRIBUTE_RECALL_ON_OPEN: u32 = 0x0004_0000;
    const FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS: u32 = 0x0040_0000;

    const MASK: u32 = FILE_ATTRIBUTE_OFFLINE
        | FILE_ATTRIBUTE_RECALL_ON_OPEN
        | FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS;

    md.file_attributes() & MASK != 0
}

#[cfg(not(windows))]
pub fn is_dehydrated(_md: &std::fs::Metadata) -> bool {
    false
}

/// Whether an IO error is transient enough to be worth retrying.
///
/// Values verified against `winerror.h` (Windows SDK 10.0.22621.0).
#[cfg(windows)]
pub fn is_retryable(e: &std::io::Error) -> bool {
    // Transient cloud-provider conditions.
    const CLOUD_FILE_PROVIDER_NOT_RUNNING: i32 = 362;
    const CLOUD_FILE_INSUFFICIENT_RESOURCES: i32 = 387;
    const CLOUD_FILE_NETWORK_UNAVAILABLE: i32 = 388;
    const CLOUD_FILE_UNSUCCESSFUL: i32 = 389;
    const CLOUD_FILE_IN_USE: i32 = 391;
    const CLOUD_FILE_REQUEST_ABORTED: i32 = 393;
    const CLOUD_FILE_PROPERTY_LOCK_CONFLICT: i32 = 397;
    const CLOUD_FILE_REQUEST_CANCELED: i32 = 398;
    const CLOUD_FILE_PROVIDER_TERMINATED: i32 = 404;
    const CLOUD_FILE_REQUEST_TIMEOUT: i32 = 426;
    const CLOUD_FILE_US_MESSAGE_TIMEOUT: i32 = 475;

    // Generic transient conditions that also show up mid-hydration.
    const SHARING_VIOLATION: i32 = 32;
    const LOCK_VIOLATION: i32 = 33;
    const UNEXP_NET_ERR: i32 = 59;
    const NETNAME_DELETED: i32 = 64;
    const SEM_TIMEOUT: i32 = 121;
    const NO_SYSTEM_RESOURCES: i32 = 1450;

    matches!(
        e.raw_os_error(),
        Some(
            CLOUD_FILE_PROVIDER_NOT_RUNNING
                | CLOUD_FILE_INSUFFICIENT_RESOURCES
                | CLOUD_FILE_NETWORK_UNAVAILABLE
                | CLOUD_FILE_UNSUCCESSFUL
                | CLOUD_FILE_IN_USE
                | CLOUD_FILE_REQUEST_ABORTED
                | CLOUD_FILE_PROPERTY_LOCK_CONFLICT
                | CLOUD_FILE_REQUEST_CANCELED
                | CLOUD_FILE_PROVIDER_TERMINATED
                | CLOUD_FILE_REQUEST_TIMEOUT
                | CLOUD_FILE_US_MESSAGE_TIMEOUT
                | SHARING_VIOLATION
                | LOCK_VIOLATION
                | UNEXP_NET_ERR
                | NETNAME_DELETED
                | SEM_TIMEOUT
                | NO_SYSTEM_RESOURCES
        )
    )
}

#[cfg(not(windows))]
pub fn is_retryable(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::WouldBlock
    )
}
