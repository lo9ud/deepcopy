use std::sync::atomic::Ordering;
use std::sync::Arc;

use clap::Parser;
use indicatif::HumanBytes;
use log::{error, info};

mod cli;
mod conflict_resolver;
mod fsattr;
mod job;
mod logger;
mod progress;
mod scheduler;
mod walker;

use conflict_resolver::{ConflictArbiter, ConflictResolutionStrategy};
use progress::Progress;
use scheduler::{Completion, Gate, Lanes, RunConfig, Stats};
use walker::WalkErrors;

/// Conflict prompts are answered one at a time, so a small queue is plenty.
const CONFLICT_QUEUE: usize = 32;

fn main() -> std::process::ExitCode {
    let args = cli::Cli::parse();

    if let Err(e) = logger::init(args.debug) {
        eprintln!("Warning: logging disabled ({e})");
    }

    if args.color_disabled() {
        console::set_colors_enabled(false);
        console::set_colors_enabled_stderr(false);
    }

    let (source, dest) = match args.resolve_paths() {
        Ok(paths) => paths,
        Err(e) => return fail(&e),
    };

    // Refuse `--conflict ask` with no terminal before anything is copied, rather than failing
    // partway through a large run.
    if let Err(e) = args.validate_interactive() {
        return fail(&e);
    }

    if !source.is_dir() {
        return fail(&format!("source is not a directory: {}", source.display()));
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => return fail(&format!("could not start the async runtime: {e}")),
    };

    let code = runtime.block_on(run(args, source, dest));

    log::logger().flush();
    code
}

async fn run(
    args: cli::Cli,
    source: std::path::PathBuf,
    dest: std::path::PathBuf,
) -> std::process::ExitCode {
    info!(
        "Copying {} -> {} ({})",
        source.display(),
        dest.display(),
        args.jobs_summary()
    );

    let progress = Progress::new(args.no_tty);
    progress.println(format!("Log file: {}", logger::LOG_FILE.display()));
    if args.dry_run {
        progress.println("DRY RUN - no files will be written");
    }

    let gate = Gate::new();
    let stats = Arc::new(Stats::default());
    let walk_errors = Arc::new(WalkErrors::default());

    // Only the interactive strategy needs an arbiter task owning stdin.
    let arbiter = if args.conflict == ConflictResolutionStrategy::Ask {
        let (tx, rx) = tokio::sync::mpsc::channel(CONFLICT_QUEUE);
        tokio::spawn(conflict_resolver::run_arbiter(
            rx,
            progress.clone(),
            gate.clone(),
            conflict_resolver::terminal_prompt(),
        ));
        ConflictArbiter::interactive(tx)
    } else {
        ConflictArbiter::fixed(args.conflict)
    };

    let roots = Arc::new(walker::Roots {
        source: source.clone(),
        target: dest.clone(),
    });

    let jobs = walker::spawn(
        roots.clone(),
        args.queue_depth,
        args.discovery_jobs,
        args.dry_run,
        progress.clone(),
        walk_errors.clone(),
    );

    // indicatif does not draw to a non-terminal, so emit periodic plain lines instead to keep
    // scheduled-task logs from being blank.
    let plain_reporter = (!progress.is_drawing()).then(|| {
        let progress = progress.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(progress.plain_interval());
            ticker.tick().await; // The first tick completes immediately.
            loop {
                ticker.tick().await;
                println!("{}", progress.plain_line());
            }
        })
    });

    let completion = scheduler::run(
        jobs,
        Lanes::new(args.local_jobs, args.cloud_jobs),
        arbiter,
        progress.clone(),
        stats.clone(),
        gate,
        RunConfig {
            roots,
            min_free: args.min_free,
            dry_run: args.dry_run,
        },
    )
    .await;

    if let Some(task) = plain_reporter {
        task.abort();
    }
    progress.finish_and_clear();

    report(
        &stats,
        &walk_errors,
        completion,
        args.dry_run,
        args.conflict,
    )
}

fn report(
    stats: &Stats,
    walk: &WalkErrors,
    completion: Completion,
    dry_run: bool,
    conflict: ConflictResolutionStrategy,
) -> std::process::ExitCode {
    // Unconditional overwrite skips the per-file existence check, so created and replaced files
    // are not counted separately. Label the line accordingly rather than implying nothing was
    // replaced.
    let merged = conflict == ConflictResolutionStrategy::Overwrite;
    let get = |v: &std::sync::atomic::AtomicU64| v.load(Ordering::Relaxed);

    let headline = match completion {
        Completion::Finished => "Done",
        Completion::Interrupted => "Interrupted",
        Completion::UserQuit => "Stopped at your request",
        Completion::OutOfSpace => "Stopped: not enough free space",
    };

    let verb = match (dry_run, merged) {
        (true, _) => "would copy",
        (false, true) => "copied/replaced",
        (false, false) => "copied",
    };
    println!("\n{headline}{}", if dry_run { " (dry run)" } else { "" });
    println!(
        "  {verb:>12}: {:>7} files  {:>10}",
        get(&stats.files_copied),
        HumanBytes(get(&stats.bytes_copied))
    );
    if !merged {
        println!(
            "  {:>12}: {:>7} files  {:>10}",
            "overwritten",
            get(&stats.files_overwritten),
            HumanBytes(get(&stats.bytes_overwritten))
        );
    }
    println!(
        "  {:>12}: {:>7} files  {:>10}",
        "skipped",
        get(&stats.files_skipped),
        HumanBytes(get(&stats.bytes_skipped))
    );

    let would_prompt = get(&stats.files_would_prompt);
    if would_prompt > 0 {
        println!(
            "  {:>12}: {:>7} files  (you would be asked about each)",
            "conflicts", would_prompt
        );
    }

    let hydrated = get(&stats.files_hydrated);
    if hydrated > 0 {
        println!(
            "  {:>12}: {:>7} files  {:>10}   <- downloaded from cloud storage",
            "hydrated",
            hydrated,
            HumanBytes(get(&stats.bytes_hydrated))
        );
    }

    let retries = get(&stats.retries);
    if retries > 0 {
        println!("  {:>12}: {retries}", "retries");
    }

    // Directory links are never descended into. Say so rather than letting an
    // incomplete copy look complete.
    let links = walk.links_skipped();
    if links > 0 {
        println!(
            "  {:>12}: {links} directory link(s) not followed; their contents were NOT copied",
            "links"
        );
    }

    let failed = get(&stats.files_failed);
    let walk_errors = walk.count();
    let mut problems = false;

    if failed > 0 {
        problems = true;
        eprintln!("  {:>12}: {failed} files", "FAILED");
    }
    if walk_errors > 0 {
        problems = true;
        eprintln!(
            "  {:>12}: {walk_errors} entries could not be read",
            "SCAN ERRORS"
        );
    }
    if problems {
        eprintln!("\nSee {} for details.", logger::LOG_FILE.display());
    }

    let clean = matches!(completion, Completion::Finished) && !problems;
    if clean {
        info!("Run completed cleanly");
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::FAILURE
    }
}

fn fail(message: &str) -> std::process::ExitCode {
    error!("{message}");
    eprintln!("error: {message}");
    std::process::ExitCode::FAILURE
}
