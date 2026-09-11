//! Parallel, streamed directory discovery.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use log::{debug, warn};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;

use crate::fsattr::is_dehydrated;
use crate::progress::Progress;

/// One file to copy.
#[derive(Debug, Clone)]
pub struct CopyJob {
    /// Path relative to the source/destination root.
    pub relative: PathBuf,
    /// Captured during the scan so no worker has to re-stat the file.
    pub bytes: u64,
    /// True for online-only cloud placeholders; selects the scheduler lane.
    pub dehydrated: bool,
}

/// The two roots a job is resolved against.
#[derive(Debug)]
pub struct Roots {
    pub source: PathBuf,
    pub target: PathBuf,
}

impl CopyJob {
    pub fn source(&self, roots: &Roots) -> PathBuf {
        roots.source.join(&self.relative)
    }

    pub fn target(&self, roots: &Roots) -> PathBuf {
        roots.target.join(&self.relative)
    }

    /// Path relative to the source root, for display.
    pub fn display_name(&self) -> String {
        self.relative.display().to_string()
    }
}

/// What the traversal could not handle.
#[derive(Default)]
pub struct WalkErrors {
    errors: AtomicU64,
    links_skipped: AtomicU64,
}

impl WalkErrors {
    pub fn count(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }

    /// Directory reparse points (junctions, directory symlinks) that were not descended into.
    pub fn links_skipped(&self) -> u64 {
        self.links_skipped.load(Ordering::Relaxed)
    }

    fn error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    fn link_skipped(&self) {
        self.links_skipped.fetch_add(1, Ordering::Relaxed);
    }
}

/// One directory's worth of findings.
#[derive(Default)]
struct Scan {
    jobs: Vec<CopyJob>,
    subdirs: Vec<PathBuf>,
}

/// Spawns discovery and returns the receiving end of the job stream.
pub fn spawn(
    roots: Arc<Roots>,
    queue_depth: usize,
    discovery_jobs: usize,
    dry_run: bool,
    progress: Progress,
    errors: Arc<WalkErrors>,
) -> mpsc::Receiver<CopyJob> {
    let (tx, rx) = mpsc::channel(queue_depth.max(1));

    tokio::spawn(async move {
        drive(
            roots,
            tx,
            progress.clone(),
            errors,
            discovery_jobs.max(1),
            dry_run,
        )
        .await;
        progress.scan_complete();
    });

    rx
}

async fn drive(
    roots: Arc<Roots>,
    tx: mpsc::Sender<CopyJob>,
    progress: Progress,
    errors: Arc<WalkErrors>,
    concurrency: usize,
    dry_run: bool,
) {
    let permits = Arc::new(Semaphore::new(concurrency));
    let mut scans: JoinSet<Scan> = JoinSet::new();

    let spawn_scan = |scans: &mut JoinSet<Scan>, dir: PathBuf| {
        let roots = roots.clone();
        let errors = errors.clone();
        let permits = permits.clone();
        scans.spawn(async move {
            let _permit = permits.acquire().await;
            // The syscalls belong on a blocking thread, not a runtime worker.
            tokio::task::spawn_blocking(move || scan_dir(&dir, &roots, &errors, dry_run))
                .await
                .unwrap_or_default()
        });
    };

    spawn_scan(&mut scans, roots.source.clone());

    while let Some(joined) = scans.join_next().await {
        let scan = match joined {
            Ok(s) => s,
            Err(e) => {
                errors.error();
                warn!("Directory scan task failed: {e}");
                continue;
            }
        };

        for job in scan.jobs {
            progress.found(job.bytes, job.dehydrated);
            // Await here parks until queue has space available
            if tx.send(job).await.is_err() {
                debug!("Receiver dropped; stopping discovery");
                return;
            }
        }

        for dir in scan.subdirs {
            spawn_scan(&mut scans, dir);
        }
    }
}

/// Reads one directory. Blocking; runs on a blocking thread.
///
/// Also mirrors the directory itself into the destination.
fn scan_dir(dir: &Path, roots: &Roots, errors: &WalkErrors, dry_run: bool) -> Scan {
    let mut scan = Scan::default();
    let source_root = &roots.source;

    if !dry_run {
        if let Ok(rel) = dir.strip_prefix(source_root) {
            let mirrored = roots.target.join(rel);
            if let Err(e) = std::fs::create_dir_all(&mirrored) {
                errors.error();
                warn!("Could not create directory {}: {e}", mirrored.display());
            }
        }
    }

    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            errors.error();
            warn!("Could not read directory {}: {e}", dir.display());
            return scan;
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                errors.error();
                warn!("Could not read an entry in {}: {e}", dir.display());
                continue;
            }
        };

        // On Windows this comes from the directory enumeration itself, so it costs no extra
        // syscall and never opens the file, so it cannot trigger hydration.
        let metadata = match entry.metadata() {
            Ok(m) => m,
            Err(e) => {
                errors.error();
                warn!("Could not stat {}: {e}", entry.path().display());
                continue;
            }
        };

        let path = entry.path();
        let file_type = metadata.file_type();

        if file_type.is_symlink() {
            if path.is_dir() {
                errors.link_skipped();
                warn!(
                    "Not descending into directory link {} (its contents are NOT copied)",
                    path.display()
                );
                continue;
            }
            // A link to a file is fine: std::fs::copy follows it and copies the contents.
        }

        if file_type.is_dir() {
            scan.subdirs.push(path);
            continue;
        }

        let relative = match path.strip_prefix(source_root) {
            Ok(r) => r,
            Err(e) => {
                errors.error();
                warn!("Path {} escaped the source root: {e}", path.display());
                continue;
            }
        };

        scan.jobs.push(CopyJob {
            relative: relative.to_path_buf(),
            bytes: metadata.len(),
            dehydrated: is_dehydrated(&metadata),
        });
    }

    scan
}

#[cfg(test)]
mod tests {
    use super::*;

    const JOBS: usize = 4;

    fn tree() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("src/nested/deep")).unwrap();
        std::fs::write(dir.path().join("src/a.txt"), b"aaa").unwrap();
        std::fs::write(dir.path().join("src/nested/b.txt"), b"bbbb").unwrap();
        std::fs::write(dir.path().join("src/nested/deep/c.txt"), b"ccccc").unwrap();
        dir
    }

    async fn collect(
        source: PathBuf,
        target: PathBuf,
        depth: usize,
    ) -> (Vec<CopyJob>, Arc<WalkErrors>) {
        let errors = Arc::new(WalkErrors::default());
        let mut rx = spawn(
            Arc::new(Roots { source, target }),
            depth,
            JOBS,
            false,
            Progress::default(),
            errors.clone(),
        );
        let mut out = Vec::new();
        while let Some(job) = rx.recv().await {
            out.push(job);
        }
        (out, errors)
    }

    #[tokio::test]
    async fn discovers_every_file_and_mirrors_structure() {
        let dir = tree();
        let source = dir.path().join("src");
        let target = dir.path().join("dst");
        let (found, errors) = collect(source.clone(), target.clone(), 16).await;

        assert_eq!(found.len(), 3, "directories must not become jobs");
        assert_eq!(errors.count(), 0);
        assert_eq!(
            found.iter().map(|j| j.bytes).sum::<u64>(),
            3 + 4 + 5,
            "sizes captured during the scan"
        );

        let roots = Roots {
            source: source.clone(),
            target: target.clone(),
        };
        let deep = found
            .iter()
            .find(|j| j.relative.ends_with("c.txt"))
            .expect("nested file discovered");
        assert_eq!(
            deep.target(&roots),
            target.join("nested").join("deep").join("c.txt")
        );
        assert_eq!(
            deep.display_name(),
            ["nested", "deep", "c.txt"].join(std::path::MAIN_SEPARATOR_STR)
        );
    }

    #[tokio::test]
    async fn root_itself_is_not_a_job() {
        let dir = tree();
        let (found, _) = collect(dir.path().join("src"), dir.path().join("dst"), 16).await;
        let roots = Roots {
            source: dir.path().join("src"),
            target: dir.path().join("dst"),
        };
        assert!(found.iter().all(|j| j.source(&roots).is_file()));
    }

    /// A copy must mirror the tree, including directories that contain no files. Creating
    /// destination directories only as a side effect of copying files into them dropped them.
    #[tokio::test]
    async fn empty_directories_are_mirrored() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("src");
        let target = dir.path().join("dst");
        std::fs::create_dir_all(source.join("empty").join("nested_empty")).unwrap();
        std::fs::create_dir_all(source.join("hasfiles")).unwrap();
        std::fs::write(source.join("hasfiles").join("a.txt"), b"hi").unwrap();

        let (found, errors) = collect(source, target.clone(), 64).await;
        assert_eq!(found.len(), 1, "only the one real file becomes a job");
        assert_eq!(errors.count(), 0);

        assert!(
            target.join("empty").is_dir(),
            "empty directory not mirrored"
        );
        assert!(
            target.join("empty").join("nested_empty").is_dir(),
            "nested empty directory not mirrored"
        );
        assert!(target.join("hasfiles").is_dir());
    }

    /// A dry run must not create anything, empty directories included.
    #[tokio::test]
    async fn dry_run_mirrors_no_directories() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("src");
        let target = dir.path().join("dst");
        std::fs::create_dir_all(source.join("empty")).unwrap();

        let errors = Arc::new(WalkErrors::default());
        let mut rx = spawn(
            Arc::new(Roots {
                source,
                target: target.clone(),
            }),
            64,
            JOBS,
            true,
            Progress::default(),
            errors,
        );
        while rx.recv().await.is_some() {}

        assert!(!target.exists(), "dry run created the destination tree");
    }

    /// A directory link must never be mistaken for a regular file. std reports a junction as a
    /// symlink whose `is_dir()` is false, so an `is_dir()`-first check would try to copy it.
    #[cfg(windows)]
    #[tokio::test]
    async fn directory_links_are_reported_not_copied() {
        let dir = tree();
        let source = dir.path().join("src");
        let link = source.join("link");

        let status = std::process::Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(&link)
            .arg(dir.path().join("src").join("nested"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();

        if !matches!(status, Ok(s) if s.success()) {
            eprintln!("skipping: could not create a junction here");
            return;
        }

        let (jobs, errors) = collect(source, dir.path().join("dst"), 16).await;
        assert_eq!(errors.links_skipped(), 1, "the junction must be counted");
        assert_eq!(errors.count(), 0, "a junction is not a traversal error");
        assert!(
            !jobs.iter().any(|j| j.relative == Path::new("link")),
            "the junction must not become a copy job"
        );
        assert_eq!(jobs.len(), 3, "the three real files are still discovered");
    }
}