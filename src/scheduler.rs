//! Two-lane dispatch for network I/O (dehydrated files, online) and disk I/O (hydrated or local files)

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use log::{debug, info};
use tokio::sync::{mpsc, Notify, Semaphore};
use tokio::task::JoinSet;

use crate::conflict_resolver::ConflictArbiter;
use crate::job::{self, DirCache, Outcome};
use crate::progress::Progress;
use crate::walker::{CopyJob, Roots};

/// How often the free-space guard re-checks, in completed files.
const SPACE_CHECK_INTERVAL: u64 = 500;

#[derive(Default)]
pub struct Stats {
    pub files_copied: AtomicU64,
    pub files_skipped: AtomicU64,
    pub files_overwritten: AtomicU64,
    pub files_failed: AtomicU64,
    /// Dry run only: conflicts that would have prompted.
    pub files_would_prompt: AtomicU64,

    pub bytes_copied: AtomicU64,
    pub bytes_skipped: AtomicU64,
    pub bytes_overwritten: AtomicU64,

    /// Bytes pulled down from cloud storage.
    pub bytes_hydrated: AtomicU64,
    pub files_hydrated: AtomicU64,

    pub retries: AtomicU64,
}

impl Stats {
    fn record(&self, job: &CopyJob, result: &job::JobResult) {
        let bytes = job.bytes;
        self.retries
            .fetch_add(result.retries as u64, Ordering::Relaxed);

        match result.outcome {
            Outcome::Copied => {
                self.files_copied.fetch_add(1, Ordering::Relaxed);
                self.bytes_copied.fetch_add(bytes, Ordering::Relaxed);
            }
            Outcome::Overwritten => {
                self.files_overwritten.fetch_add(1, Ordering::Relaxed);
                self.bytes_overwritten.fetch_add(bytes, Ordering::Relaxed);
            }
            Outcome::Skipped => {
                self.files_skipped.fetch_add(1, Ordering::Relaxed);
                self.bytes_skipped.fetch_add(bytes, Ordering::Relaxed);
                return; // Nothing was transferred, so nothing was hydrated.
            }
            Outcome::Failed => {
                self.files_failed.fetch_add(1, Ordering::Relaxed);
                return;
            }
            Outcome::WouldPrompt => {
                self.files_would_prompt.fetch_add(1, Ordering::Relaxed);
                return;
            }
            Outcome::Quit => return,
        }

        if job.dehydrated {
            self.files_hydrated.fetch_add(1, Ordering::Relaxed);
            self.bytes_hydrated.fetch_add(bytes, Ordering::Relaxed);
        }
    }
}

/// A latch that holds new dispatch while something else owns the terminal.
#[derive(Clone, Default)]
pub struct Gate {
    held: Arc<AtomicBool>,
    released: Arc<Notify>,
}

pub struct GateHold(Gate);

impl Gate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Holds the gate until the returned guard is dropped.
    pub fn hold(&self) -> GateHold {
        self.held.store(true, Ordering::SeqCst);
        GateHold(self.clone())
    }

    async fn wait_until_open(&self) {
        loop {
            let notified = self.released.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if !self.held.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for GateHold {
    fn drop(&mut self) {
        self.0.held.store(false, Ordering::SeqCst);
        self.0.released.notify_waiters();
    }
}

/// Why the run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completion {
    Finished,
    Interrupted,
    UserQuit,
    OutOfSpace,
}

pub struct Lanes {
    local: Arc<Semaphore>,
    cloud: Arc<Semaphore>,
}

impl Lanes {
    pub fn new(local_jobs: usize, cloud_jobs: usize) -> Self {
        Self {
            local: Arc::new(Semaphore::new(local_jobs.max(1))),
            cloud: Arc::new(Semaphore::new(cloud_jobs.max(1))),
        }
    }

    fn for_job(&self, job: &CopyJob) -> Arc<Semaphore> {
        if job.dehydrated {
            self.cloud.clone()
        } else {
            self.local.clone()
        }
    }
}

pub struct RunConfig {
    pub roots: Arc<Roots>,
    pub min_free: u64,
    pub dry_run: bool,
}

/// Drains the discovery stream, dispatching each job into its lane.
pub async fn run(
    mut jobs: mpsc::Receiver<CopyJob>,
    lanes: Lanes,
    arbiter: ConflictArbiter,
    progress: Progress,
    stats: Arc<Stats>,
    gate: Gate,
    config: RunConfig,
) -> Completion {
    let mut tasks: JoinSet<()> = JoinSet::new();
    let mut completion = Completion::Finished;
    let quit = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicU64::new(0));

    let dirs = Arc::new(DirCache::default());
    let mut next_space_check = SPACE_CHECK_INTERVAL;

    // Fail fast if the volumes are already short of room before copying anything.
    if low_on_space(&config, &progress) {
        return Completion::OutOfSpace;
    }

    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

    loop {
        // Never start new work while a prompt owns the terminal.
        gate.wait_until_open().await;

        if quit.load(Ordering::SeqCst) {
            completion = Completion::UserQuit;
            break;
        }

        let job = tokio::select! {
            biased;
            _ = &mut ctrl_c => {
                info!("Interrupt received; finishing in-flight copies");
                completion = Completion::Interrupted;
                break;
            }
            job = jobs.recv() => job,
        };

        let Some(job) = job else {
            break; // Walker finished and dropped the sender.
        };

        let permit = match lanes.for_job(&job).acquire_owned().await {
            Ok(p) => p,
            Err(_) => break, // Semaphore closed.
        };

        // Periodically verify both volumes still have room. Streamed discovery means the total
        // size is not known up front, so one check at startup cannot cover the whole run.
        let done = completed.load(Ordering::Relaxed);
        if done >= next_space_check {
            next_space_check = done + SPACE_CHECK_INTERVAL;
            if low_on_space(&config, &progress) {
                completion = Completion::OutOfSpace;
                break;
            }
        }

        let arbiter = arbiter.clone();
        let progress = progress.clone();
        let stats = stats.clone();
        let quit = quit.clone();
        let completed = completed.clone();
        let dry_run = config.dry_run;
        let roots = config.roots.clone();
        let dirs = dirs.clone();

        // Reap finished tasks as we go.
        while tasks.try_join_next().is_some() {}

        tasks.spawn(async move {
            let _permit = permit; // Released when this task ends.

            debug!("Starting {}", job.display_name());
            let result = job::run(&job, &roots, &arbiter, &dirs, dry_run).await;

            if result.outcome == Outcome::Quit {
                quit.store(true, Ordering::SeqCst);
            }

            stats.record(&job, &result);
            completed.fetch_add(1, Ordering::Relaxed);
            progress.finished(job.bytes);
        });
    }

    // Let in-flight copies finish rather than tearing them down mid-write.
    if completion != Completion::Finished {
        info!("Waiting for {} in-flight copies to finish", tasks.len());
    }
    while tasks.join_next().await.is_some() {}

    if quit.load(Ordering::SeqCst) && completion == Completion::Finished {
        completion = Completion::UserQuit;
    }
    completion
}

/// Checks both volumes.
///
/// Rehydration can fill source, so guards against source volume being filled as well.
fn low_on_space(config: &RunConfig, progress: &Progress) -> bool {
    for (label, path) in [
        ("destination", &config.roots.target),
        ("source", &config.roots.source),
    ] {
        match fs4::available_space(path) {
            Ok(free) if free < config.min_free => {
                progress.println(format!(
                    "Stopping: {label} volume has {} free, below the {} minimum.",
                    indicatif::HumanBytes(free),
                    indicatif::HumanBytes(config.min_free),
                ));
                return true;
            }
            Ok(_) => {}
            Err(e) => debug!("Could not query free space for {}: {e}", path.display()),
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn gate_blocks_until_hold_is_dropped() {
        let gate = Gate::new();
        let hold = gate.hold();

        let waiter = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.wait_until_open().await })
        };

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!waiter.is_finished(), "gate should still be held");

        drop(hold);
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("gate should release")
            .unwrap();
    }

    /// Concurrency smoke test: many waiters, repeated hold/release, nothing stalls.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn gate_survives_repeated_contended_release() {
        let gate = Gate::new();

        for _ in 0..200 {
            let hold = gate.hold();
            let waiters: Vec<_> = (0..8)
                .map(|_| {
                    let gate = gate.clone();
                    tokio::spawn(async move { gate.wait_until_open().await })
                })
                .collect();

            tokio::task::yield_now().await;
            drop(hold);

            for w in waiters {
                tokio::time::timeout(std::time::Duration::from_secs(5), w)
                    .await
                    .expect("no waiter may stall")
                    .unwrap();
            }
        }
    }

    #[tokio::test]
    async fn open_gate_does_not_block() {
        let gate = Gate::new();
        tokio::time::timeout(
            std::time::Duration::from_millis(200),
            gate.wait_until_open(),
        )
        .await
        .expect("an unheld gate must not block");
    }
}
