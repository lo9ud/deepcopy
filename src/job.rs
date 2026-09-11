//! Execution of a single copy, with retry.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use log::{debug, warn};

use crate::conflict_resolver::{ConflictArbiter, ConflictResolution};
use crate::fsattr::is_retryable;
use crate::walker::{CopyJob, Roots};
use std::sync::Arc;

const MAX_ATTEMPTS: u32 = 5;
const INITIAL_BACKOFF: Duration = Duration::from_secs(2);

/// Cap on remembered directories, so a tree with a pathological number of them cannot grow this
/// without bound.
const MAX_REMEMBERED_DIRS: usize = 100_000;

/// Remembers which destination directories already exist.
#[derive(Default)]
pub struct DirCache {
    seen: Mutex<HashSet<PathBuf>>,
}

impl DirCache {
    /// Ensures `dir` exists, skipping the syscall if we already created it this run.
    pub fn ensure(&self, dir: &Path) -> std::io::Result<()> {
        if self.seen.lock().is_ok_and(|g| g.contains(dir)) {
            return Ok(());
        }
        std::fs::create_dir_all(dir)?;
        if let Ok(mut g) = self.seen.lock() {
            if g.len() >= MAX_REMEMBERED_DIRS {
                g.clear();
            }
            g.insert(dir.to_path_buf());
        }
        Ok(())
    }

    /// Forgets a directory so the next `ensure` creates it.
    fn forget(&self, dir: &Path) {
        if let Ok(mut g) = self.seen.lock() {
            g.remove(dir);
        }
    }
}

/// What happened to one file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Copied,
    Overwritten,
    Skipped,
    Failed,
    /// Dry run only: a conflict the user would have been asked about.
    WouldPrompt,
    /// The user chose to stop the run at a conflict prompt.
    Quit,
}

pub struct JobResult {
    pub outcome: Outcome,
    pub retries: u32,
}

/// Copies one file, resolving any conflict first.
///
/// The job stores only a relative path, so both endpoints are resolved here.
pub async fn run(
    job: &CopyJob,
    roots: &Roots,
    arbiter: &ConflictArbiter,
    dirs: &Arc<DirCache>,
    dry_run: bool,
) -> JobResult {
    let mut retries = 0;
    let source = job.source(roots);
    let target = job.target(roots);

    // Under an unconditional overwrite the destination is replaced either way, so the existence
    // check is unnecessary.
    if arbiter.always_overwrites() {
        if dry_run {
            return JobResult {
                outcome: Outcome::Copied,
                retries,
            };
        }
        return match copy_with_retry(&source, &target, dirs, &mut retries).await {
            Ok(()) => JobResult {
                outcome: Outcome::Copied,
                retries,
            },
            Err(e) => {
                warn!(
                    "Failed to copy {} -> {}: {e}",
                    source.display(),
                    target.display()
                );
                JobResult {
                    outcome: Outcome::Failed,
                    retries,
                }
            }
        };
    }

    if target.exists() {
        if dry_run && arbiter.prompts() {
            return JobResult {
                outcome: Outcome::WouldPrompt,
                retries,
            };
        }

        match arbiter.resolve(&source, &target).await {
            ConflictResolution::Skip => {
                debug!("Skipping existing {}", target.display());
                return JobResult {
                    outcome: Outcome::Skipped,
                    retries,
                };
            }
            ConflictResolution::Quit => {
                return JobResult {
                    outcome: Outcome::Quit,
                    retries,
                };
            }
            ConflictResolution::Overwrite => {
                if dry_run {
                    return JobResult {
                        outcome: Outcome::Overwritten,
                        retries,
                    };
                }
                return match copy_with_retry(&source, &target, dirs, &mut retries).await {
                    Ok(()) => JobResult {
                        outcome: Outcome::Overwritten,
                        retries,
                    },
                    Err(e) => {
                        warn!(
                            "Failed to overwrite {} -> {}: {e}",
                            source.display(),
                            target.display()
                        );
                        JobResult {
                            outcome: Outcome::Failed,
                            retries,
                        }
                    }
                };
            }
        }
    }

    if dry_run {
        return JobResult {
            outcome: Outcome::Copied,
            retries,
        };
    }

    match copy_with_retry(&source, &target, dirs, &mut retries).await {
        Ok(()) => JobResult {
            outcome: Outcome::Copied,
            retries,
        },
        Err(e) => {
            warn!(
                "Failed to copy {} -> {}: {e}",
                source.display(),
                target.display()
            );
            JobResult {
                outcome: Outcome::Failed,
                retries,
            }
        }
    }
}

/// Retries only on transient errors. 
/// 
/// There is deliberately no timeout as rehydration can take arbitrarily long under bad network conditions.
async fn copy_with_retry(
    src: &Path,
    dst: &Path,
    dirs: &Arc<DirCache>,
    retries: &mut u32,
) -> std::io::Result<()> {
    let mut backoff = INITIAL_BACKOFF;

    for attempt in 1..=MAX_ATTEMPTS {
        let source = src.to_path_buf();
        let target = dst.to_path_buf();

        let dirs_for_task = dirs.clone();
        let result = tokio::task::spawn_blocking(move || {
            let parent = target.parent();
            if let Some(parent) = parent {
                dirs_for_task.ensure(parent)?;
            }

            match std::fs::copy(&source, &target) {
                Ok(_) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    let Some(parent) = parent else { return Err(e) };
                    dirs_for_task.forget(parent);
                    dirs_for_task.ensure(parent)?;
                    std::fs::copy(&source, &target).map(|_| ())
                }
                Err(e) => Err(e),
            }
        })
        .await;

        let error = match result {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(e)) => e,
            Err(join) => std::io::Error::other(format!("copy task failed: {join}")),
        };

        if attempt == MAX_ATTEMPTS || !is_retryable(&error) {
            // A failed attempt can leave a partial file behind.
            cleanup_partial(dst);
            return Err(error);
        }

        *retries += 1;
        warn!(
            "Attempt {attempt}/{MAX_ATTEMPTS} for {} failed ({error}); retrying in {:?}",
            src.display(),
            backoff
        );
        tokio::time::sleep(backoff).await;
        backoff *= 2;
    }

    unreachable!("loop returns on the final attempt")
}

/// Removes a partially-written destination so an interrupted run never leaves a truncated file
/// that a later run would mistake for a complete copy.
pub fn cleanup_partial(target: &Path) {
    if target.exists() {
        if let Err(e) = std::fs::remove_file(target) {
            warn!("Could not remove partial file {}: {e}", target.display());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conflict_resolver::ConflictResolutionStrategy;
    use crate::walker::{CopyJob, Roots};
    use std::path::PathBuf;
    use std::sync::Arc;

    /// A source root, a dest root, and helpers to place files under each.
    struct Fixture {
        _dir: tempfile::TempDir,
        roots: Roots,
        dirs: Arc<DirCache>,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let roots = Roots {
                source: dir.path().join("src"),
                target: dir.path().join("dst"),
            };
            std::fs::create_dir_all(&roots.source).unwrap();
            std::fs::create_dir_all(&roots.target).unwrap();
            Self {
                _dir: dir,
                roots,
                dirs: Arc::new(DirCache::default()),
            }
        }

        fn write_source(&self, rel: &str, bytes: &[u8]) -> CopyJob {
            let p = self.roots.source.join(rel);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(&p, bytes).unwrap();
            CopyJob {
                relative: PathBuf::from(rel),
                bytes: bytes.len() as u64,
                dehydrated: false,
            }
        }

        fn write_target(&self, rel: &str, bytes: &[u8]) {
            let p = self.roots.target.join(rel);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(&p, bytes).unwrap();
        }

        fn target(&self, rel: &str) -> PathBuf {
            self.roots.target.join(rel)
        }
    }

    #[tokio::test]
    async fn copies_a_new_file() {
        let f = Fixture::new();
        let job = f.write_source("a.txt", b"hello");
        let arbiter = ConflictArbiter::fixed(ConflictResolutionStrategy::Skip);
        let r = run(&job, &f.roots, &arbiter, &f.dirs, false).await;
        assert_eq!(r.outcome, Outcome::Copied);
        assert_eq!(std::fs::read(f.target("a.txt")).unwrap(), b"hello");
    }

    #[tokio::test]
    async fn creates_missing_parent_directories() {
        let f = Fixture::new();
        let job = f.write_source("deeply/nested/out/a.txt", b"x");
        let arbiter = ConflictArbiter::fixed(ConflictResolutionStrategy::Skip);
        assert_eq!(
            run(&job, &f.roots, &arbiter, &f.dirs, false).await.outcome,
            Outcome::Copied
        );
        assert!(f.target("deeply/nested/out/a.txt").exists());
    }

    #[tokio::test]
    async fn dry_run_does_not_create_files() {
        let f = Fixture::new();
        let job = f.write_source("a.txt", b"hello");
        let arbiter = ConflictArbiter::fixed(ConflictResolutionStrategy::Skip);
        let r = run(&job, &f.roots, &arbiter, &f.dirs, true).await;
        assert_eq!(r.outcome, Outcome::Copied);
        assert!(!f.target("a.txt").exists(), "dry run must not write");
    }

    #[tokio::test]
    async fn dry_run_does_not_overwrite_existing_files() {
        let f = Fixture::new();
        let job = f.write_source("a.txt", b"new contents");
        f.write_target("a.txt", b"ORIGINAL");
        let arbiter = ConflictArbiter::fixed(ConflictResolutionStrategy::Overwrite);
        let r = run(&job, &f.roots, &arbiter, &f.dirs, true).await;
        // Reported as Copied, not Overwritten: unconditional overwrite skips the existence
        // check, so "created" and "replaced" are counted the same.
        assert_eq!(r.outcome, Outcome::Copied);
        assert_eq!(
            std::fs::read(f.target("a.txt")).unwrap(),
            b"ORIGINAL",
            "dry run must leave the destination byte-identical"
        );
    }

    #[tokio::test]
    async fn overwrite_replaces_destination() {
        let f = Fixture::new();
        let job = f.write_source("a.txt", b"new");
        f.write_target("a.txt", b"a longer old value");
        let arbiter = ConflictArbiter::fixed(ConflictResolutionStrategy::Overwrite);
        // Counted as Copied: see dry_run_does_not_overwrite_existing_files.
        assert_eq!(
            run(&job, &f.roots, &arbiter, &f.dirs, false).await.outcome,
            Outcome::Copied
        );
        assert_eq!(std::fs::read(f.target("a.txt")).unwrap(), b"new");
    }

    #[tokio::test]
    async fn skip_leaves_destination_alone() {
        let f = Fixture::new();
        let job = f.write_source("a.txt", b"new");
        f.write_target("a.txt", b"old");
        let arbiter = ConflictArbiter::fixed(ConflictResolutionStrategy::Skip);
        assert_eq!(
            run(&job, &f.roots, &arbiter, &f.dirs, false).await.outcome,
            Outcome::Skipped
        );
        assert_eq!(std::fs::read(f.target("a.txt")).unwrap(), b"old");
    }

    #[tokio::test]
    async fn missing_source_fails_without_panicking() {
        let f = Fixture::new();
        let job = CopyJob {
            relative: PathBuf::from("nope.txt"),
            bytes: 0,
            dehydrated: false,
        };
        let arbiter = ConflictArbiter::fixed(ConflictResolutionStrategy::Skip);
        let r = run(&job, &f.roots, &arbiter, &f.dirs, false).await;
        assert_eq!(r.outcome, Outcome::Failed);
        assert_eq!(r.retries, 0, "a missing file is not a transient error");
    }

    /// The directory cache must not change behaviour: a deep new path is still created.
    #[tokio::test]
    async fn dir_cache_still_creates_every_needed_directory() {
        let f = Fixture::new();
        let arbiter = ConflictArbiter::fixed(ConflictResolutionStrategy::Skip);
        for rel in ["x/1/a.txt", "x/2/b.txt", "y/c.txt", "x/1/d.txt"] {
            let job = f.write_source(rel, b"data");
            assert_eq!(
                run(&job, &f.roots, &arbiter, &f.dirs, false).await.outcome,
                Outcome::Copied
            );
            assert!(f.target(rel).exists(), "{rel} not created");
        }
    }

    /// A directory vanishing mid-run must not leave a stale cache entry that breaks later files.
    #[tokio::test]
    async fn dir_cache_recovers_if_a_directory_disappears() {
        let f = Fixture::new();
        let arbiter = ConflictArbiter::fixed(ConflictResolutionStrategy::Skip);

        let first = f.write_source("gone/a.txt", b"one");
        assert_eq!(
            run(&first, &f.roots, &arbiter, &f.dirs, false)
                .await
                .outcome,
            Outcome::Copied
        );

        // Remove the directory behind the cache's back.
        std::fs::remove_dir_all(f.roots.target.join("gone")).unwrap();

        let second = f.write_source("gone/b.txt", b"two");
        assert_eq!(
            run(&second, &f.roots, &arbiter, &f.dirs, false)
                .await
                .outcome,
            Outcome::Copied,
            "a stale cache entry must not make later copies fail"
        );
        assert!(f.target("gone/b.txt").exists());
    }

    /// Overwrite mode must still replace a file it never stat-ed.
    #[tokio::test]
    async fn overwrite_without_the_existence_check_still_replaces() {
        let f = Fixture::new();
        let job = f.write_source("a.txt", b"fresh");
        f.write_target("a.txt", b"stale and much longer");
        let arbiter = ConflictArbiter::fixed(ConflictResolutionStrategy::Overwrite);
        assert!(arbiter.always_overwrites());
        run(&job, &f.roots, &arbiter, &f.dirs, false).await;
        assert_eq!(std::fs::read(f.target("a.txt")).unwrap(), b"fresh");
    }

    /// Both endpoints come from the roots, so a nested relative path lands in the mirrored
    /// location rather than being flattened.
    #[tokio::test]
    async fn relative_path_resolves_against_both_roots() {
        let f = Fixture::new();
        let job = f.write_source("one/two/three.txt", b"deep");
        assert_eq!(
            job.source(&f.roots),
            f.roots.source.join("one").join("two").join("three.txt")
        );
        assert_eq!(
            job.target(&f.roots),
            f.roots.target.join("one").join("two").join("three.txt")
        );
        assert!(job.display_name().ends_with("three.txt"));
    }
}
