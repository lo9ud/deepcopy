use std::sync::{Arc, Mutex};

use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use threadpool::ThreadPool;
mod cli;
mod conflict_resolver;
mod copy_queue;

struct ProgressHolder {
    pb: std::sync::Arc<std::sync::Mutex<ProgressBar>>,
    multi_progress: std::sync::Arc<std::sync::Mutex<MultiProgress>>,
}

impl ProgressHolder {
    fn new(n_jobs: u64) -> Self {
        let pb = indicatif::ProgressBar::with_draw_target(Some(n_jobs), ProgressDrawTarget::stdout());
        pb.set_style(
            ProgressStyle::default_bar()
                .progress_chars("█▓▒░ ")
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
                .template("{spinner:.green} [{elapsed_precise}]▕{bar:85.blue/green}▏{pos}/{len} files {wide_msg}")
                .expect("Failed to set style"),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(150));
        let mpb = indicatif::MultiProgress::new();
        mpb.add(pb.clone());
        Self {
            pb: Arc::new(Mutex::new(pb)),
            multi_progress: Arc::new(Mutex::new(mpb)),
        }
    }

    fn add(&self, len: u64, name: String) -> ProgressBar {
        let pb = self
            .multi_progress
            .lock()
            .unwrap()
            .add(ProgressBar::with_draw_target(
                Some(len),
                ProgressDrawTarget::stdout(),
            ));
        pb.set_style(
            ProgressStyle::default_bar()
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
                .template("↪ {spinner:.green} [{elapsed_precise}] ({total_bytes:>10}) {wide_msg}")
                .expect("Failed to set style"),
        );
        pb.set_message(name);
        pb.enable_steady_tick(std::time::Duration::from_millis(150));
        pb
    }

    fn remove(&self, pb: &ProgressBar) {
        self.multi_progress.lock().unwrap().remove(pb);
    }

    fn inc_main(&self) {
        self.pb.lock().unwrap().inc(1);
    }
}

struct Stats {
    files: u64,
    files_skipped: u64,
    files_overwritten: u64,

    bytes: u64,
    bytes_skipped: u64,
    bytes_overwritten: u64,
}

impl Stats {
    fn new() -> Self {
        Self {
            files: 0,
            files_skipped: 0,
            files_overwritten: 0,

            bytes: 0,
            bytes_skipped: 0,
            bytes_overwritten: 0,
        }
    }
}

fn main() {
    let args = cli::Cli::parse();
    let mut job_queue = copy_queue::JobQueue::new(args.source, args.dest);
    job_queue.populate();
    let pool = ThreadPool::new(4);

    let main_progress = Arc::new(ProgressHolder::new(job_queue.jobs as u64));
    let stats = Arc::new(Mutex::new(Stats::new()));
    for job in job_queue {
        let main_progress = main_progress.clone();
        let stats = stats.clone();
        pool.execute(move || {
            job._execute(args.conflict_resolution_strategy, main_progress, stats, args.dry_run);
        });
    }
    pool.join();
    main_progress.pb.lock().unwrap().finish_and_clear();
    let stats = stats.lock().unwrap();
    println!("Done! {} files copied, {} files skipped, {} files overwritten", stats.files, stats.files_skipped, stats.files_overwritten);
}
