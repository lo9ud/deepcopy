use std::io::IsTerminal;
use std::path::PathBuf;

use clap::{ArgAction, Parser, ValueHint};

use crate::conflict_resolver::ConflictResolutionStrategy;

const LONG_ABOUT: &str = "Copies a directory tree, with first-class handling for OneDrive \
Files-On-Demand placeholders.";

#[derive(Parser, Debug)]
#[command(
    about = env!("CARGO_PKG_DESCRIPTION"),
    version = env!("CARGO_PKG_VERSION"),
    author = env!("CARGO_PKG_AUTHORS"),
    long_about = LONG_ABOUT,
)]
pub struct Cli {
    /// Directory to copy from.
    #[arg(value_name = "SOURCE", value_hint = ValueHint::DirPath)]
    pub source_pos: Option<PathBuf>,

    /// Directory to copy to.
    #[arg(value_name = "DEST", value_hint = ValueHint::DirPath)]
    pub dest_pos: Option<PathBuf>,

    /// Directory to copy from (alternative to the positional form).
    #[arg(short = 's', long = "source", value_name = "SOURCE", value_hint = ValueHint::DirPath)]
    pub source_opt: Option<PathBuf>,

    /// Directory to copy to (alternative to the positional form).
    #[arg(short = 'd', long = "dest", value_name = "DEST", value_hint = ValueHint::DirPath)]
    pub dest_opt: Option<PathBuf>,

    /// What to do when a file already exists at the destination.
    #[arg(short = 'c', long = "conflict", default_value = "ask")]
    pub conflict: ConflictResolutionStrategy,

    /// Concurrent copies of locally-available files.
    #[arg(long, value_name = "N", default_value_t = default_local_jobs())]
    pub local_jobs: usize,

    /// Concurrent rehydrations of online-only files.
    #[arg(long, value_name = "N", default_value_t = 6)]
    pub cloud_jobs: usize,

    /// Abort if free space on either volume drops below this many bytes.
    #[arg(long, value_name = "BYTES", default_value_t = 1024 * 1024 * 1024)]
    pub min_free: u64,

    /// Directories scanned at once while discovering files.
    #[arg(long, value_name = "N", default_value_t = default_discovery_jobs())]
    pub discovery_jobs: usize,

    /// Ceiling on discovered-but-not-yet-copied files held in memory.
    #[arg(long, value_name = "N", default_value_t = 262_144)]
    pub queue_depth: usize,

    /// Report what would be copied without writing anything.
    #[arg(long, action = ArgAction::SetTrue)]
    pub dry_run: bool,

    /// Never emit colour, even to a terminal that supports it.
    #[arg(long, action = ArgAction::SetTrue)]
    pub no_color: bool,

    /// Render as though stdout were redirected: no progress bar, periodic plain lines instead.
    ///
    /// Also makes the run non-interactive, so `--conflict ask` is refused.
    #[arg(long, action = ArgAction::SetTrue)]
    pub no_tty: bool,

    /// Log at debug level.
    #[arg(long, action = ArgAction::SetTrue, hide = true)]
    pub debug: bool,
}

/// Directory enumeration is metadata work, so it scales past the core count -- most of the time
/// each scan is parked waiting on the filesystem rather than computing.
fn default_discovery_jobs() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get() * 2)
        .unwrap_or(8)
        .clamp(4, 32)
}

/// Copying is latency-bound rather than CPU-bound -- most of each copy is spent waiting on the
/// filesystem (and, on Windows, on the antimalware filter), so its worth oversubscribing.
fn default_local_jobs() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get() * 2)
        .unwrap_or(8)
        .clamp(4, 16)
}

impl Cli {
    /// Resolves the positional and named forms of `source`/`dest` into one pair.
    pub fn resolve_paths(&self) -> Result<(PathBuf, PathBuf), String> {
        match (
            self.source_pos.as_ref(),
            self.dest_pos.as_ref(),
            self.source_opt.as_ref(),
            self.dest_opt.as_ref(),
        ) {
            (Some(src), Some(dest), None, None) | (None, None, Some(src), Some(dest)) => {
                Ok((src.clone(), dest.clone()))
            }
            (a, b, c, d) => {
                // A positional and a named arguement used simultaneously
                if (a.is_some() || b.is_some()) && (c.is_some() || d.is_some()) {
                    Err(
                        "Exactly one of either the positional or the named form must be used."
                            .into(),
                    )
                // One of the two required values is missing
                } else if (a.is_some() ^ b.is_some()) || (c.is_some() ^ d.is_some()) {
                    // Dest is missing
                    if a.is_some() || c.is_some() {
                        Err("Missing required value dest".into())
                    // Source is missing
                    } else {
                        Err("Missing required value source".into())
                    }
                // Neither of the two required values is present
                } else if a.is_none() && b.is_none() && c.is_none() && d.is_none() {
                    Err("Missing required values source and dest".into())
                } else {
                    // Verified via truth table on:
                    // ((X1∨X2)∧(X3∨X4))
                    // OR
                    // (X1⊕X2)∨(X3⊕X4)
                    // OR
                    // ¬(X1∨X2∨X3∨X4)
                    unreachable!("All cases covered.")
                }
            }
        }
    }

    /// Whether colour should be suppressed.
    ///
    /// Honours `NO_COLOR` if set.
    pub fn color_disabled(&self) -> bool {
        self.no_color || std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty())
    }

    /// Whether this run may prompt.
    pub fn is_interactive(&self) -> bool {
        !self.no_tty && std::io::stdin().is_terminal()
    }

    /// Rejects `--conflict ask` when there is no terminal to ask on, before anything is copied.
    pub fn validate_interactive(&self) -> Result<(), String> {
        if self.conflict != ConflictResolutionStrategy::Ask || self.dry_run || self.is_interactive()
        {
            return Ok(());
        }
        let cause = if self.no_tty {
            "--no-tty was given, so this run is non-interactive"
        } else {
            "stdin is not a terminal"
        };
        Err(format!(
            "--conflict ask needs a terminal to prompt on, but {cause}.\n\
             Pass an explicit --conflict skip or --conflict overwrite for non-interactive runs."
        ))
    }

    pub fn jobs_summary(&self) -> String {
        format!(
            "{} local / {} cloud / {} discovery, queue {}",
            self.local_jobs, self.cloud_jobs, self.discovery_jobs, self.queue_depth
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("deepcopy").chain(args.iter().copied())).unwrap()
    }

    #[test]
    fn positional_form() {
        let cli = parse(&["src", "dst"]);
        let (s, d) = cli.resolve_paths().unwrap();
        assert_eq!(s, PathBuf::from("src"));
        assert_eq!(d, PathBuf::from("dst"));
    }

    #[test]
    fn named_form() {
        let cli = parse(&["-s", "src", "-d", "dst"]);
        let (s, d) = cli.resolve_paths().unwrap();
        assert_eq!(s, PathBuf::from("src"));
        assert_eq!(d, PathBuf::from("dst"));
    }

    #[test]
    fn mixing_forms_errors() {
        let cli = parse(&["src", "-d", "dst"]);
        assert!(cli
            .resolve_paths()
            .unwrap_err()
            .contains("the positional or the named"));
        let cli = parse(&["dst", "-s", "src"]);
        assert!(cli
            .resolve_paths()
            .unwrap_err()
            .contains("the positional or the named"))
    }

    #[test]
    fn missing_operand_errors() {
        let cli = parse(&["src"]);
        let err = cli.resolve_paths().unwrap_err();
        assert!(err.contains("required value dest"), "incorrect err: {err}");
    }

    #[test]
    fn conflict_defaults_to_ask() {
        assert_eq!(parse(&["a", "b"]).conflict, ConflictResolutionStrategy::Ask);
    }
}
