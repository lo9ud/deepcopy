//! Conflict policy and the interactive arbiter.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::ValueEnum;
use log::error;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ConflictResolutionStrategy {
    /// Prompt for each conflict, offering to apply the answer to all remaining conflicts.
    Ask,
    /// Always replace the destination file.
    Overwrite,
    /// Always keep the destination file.
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictResolution {
    Overwrite,
    Skip,
    /// The user asked to stop the whole run.
    Quit,
}

/// One answer the user can give at the prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    Overwrite,
    Skip,
    OverwriteAll,
    SkipAll,
    Quit,
}

impl Answer {
    const LABELS: [&str; 5] = [
        "Overwrite this file",
        "Skip this file",
        "Overwrite all remaining conflicts",
        "Skip all remaining conflicts",
        "Quit",
    ];

    pub fn from_index(i: usize) -> Self {
        match i {
            0 => Self::Overwrite,
            1 => Self::Skip,
            2 => Self::OverwriteAll,
            3 => Self::SkipAll,
            _ => Self::Quit,
        }
    }

    fn resolution(self) -> ConflictResolution {
        match self {
            Self::Overwrite | Self::OverwriteAll => ConflictResolution::Overwrite,
            Self::Skip | Self::SkipAll => ConflictResolution::Skip,
            Self::Quit => ConflictResolution::Quit,
        }
    }

    /// The decision to latch for all subsequent conflicts, if any.
    fn latch(self) -> Option<ConflictResolution> {
        match self {
            Self::OverwriteAll => Some(ConflictResolution::Overwrite),
            Self::SkipAll => Some(ConflictResolution::Skip),
            Self::Quit => Some(ConflictResolution::Quit),
            _ => None,
        }
    }
}

/// A conflict awaiting a decision.
pub struct ConflictRequest {
    pub source: PathBuf,
    pub target: PathBuf,
    pub reply: oneshot::Sender<ConflictResolution>,
}

/// Handle used by workers to ask about a conflict.
#[derive(Clone)]
pub struct ConflictArbiter {
    strategy: ConflictResolutionStrategy,
    tx: Option<mpsc::Sender<ConflictRequest>>,
}

impl ConflictArbiter {
    /// For the non-interactive strategies, no arbiter task is needed.
    pub fn fixed(strategy: ConflictResolutionStrategy) -> Self {
        Self { strategy, tx: None }
    }

    pub fn interactive(tx: mpsc::Sender<ConflictRequest>) -> Self {
        Self {
            strategy: ConflictResolutionStrategy::Ask,
            tx: Some(tx),
        }
    }

    /// Whether resolving a conflict requires asking the user.
    pub fn prompts(&self) -> bool {
        self.strategy == ConflictResolutionStrategy::Ask
    }

    /// Whether every conflict resolves to Overwrite without consulting the filesystem.
    ///
    /// Lets the caller skip its existence check entirely: the outcome cannot differ.
    pub fn always_overwrites(&self) -> bool {
        self.strategy == ConflictResolutionStrategy::Overwrite
    }

    /// Decides what to do about `target` already existing.
    pub async fn resolve(&self, source: &Path, target: &Path) -> ConflictResolution {
        match self.strategy {
            ConflictResolutionStrategy::Overwrite => ConflictResolution::Overwrite,
            ConflictResolutionStrategy::Skip => ConflictResolution::Skip,
            ConflictResolutionStrategy::Ask => {
                let Some(tx) = &self.tx else {
                    return ConflictResolution::Skip;
                };
                let (reply, answer) = oneshot::channel();
                let request = ConflictRequest {
                    source: source.to_path_buf(),
                    target: target.to_path_buf(),
                    reply,
                };
                if tx.send(request).await.is_err() {
                    // Arbiter is gone: the run is shutting down.
                    return ConflictResolution::Quit;
                }
                answer.await.unwrap_or(ConflictResolution::Quit)
            }
        }
    }
}

/// Asks the user about one conflict. Injectable so the arbiter can be tested without a terminal.
pub type Prompt = Arc<dyn Fn(&Path, &Path) -> Answer + Send + Sync>;

/// The real terminal prompt.
pub fn terminal_prompt() -> Prompt {
    Arc::new(prompt)
}

/// Owns stdin and serialises prompting. Exactly one of these runs, so concurrent workers can
/// never contend for the terminal.
pub async fn run_arbiter(
    mut rx: mpsc::Receiver<ConflictRequest>,
    progress: crate::progress::Progress,
    gate: crate::scheduler::Gate,
    prompt_fn: Prompt,
) {
    let mut latched: Option<ConflictResolution> = None;

    while let Some(req) = rx.recv().await {
        if let Some(decision) = latched {
            let _ = req.reply.send(decision);
            continue;
        }

        // Hold new dispatch while the terminal belongs to the prompt
        // In-flight copies keep running
        let _hold = gate.hold();

        let source = req.source.clone();
        let target = req.target.clone();
        let progress = progress.clone();
        let prompt_fn = prompt_fn.clone();

        // The prompt blocks on stdin for as long as the user takes to answer. Running that
        // directly on a runtime worker would park an executor thread for the whole time.
        let answer =
            tokio::task::spawn_blocking(move || progress.suspend(|| prompt_fn(&source, &target)))
                .await
                .unwrap_or(Answer::Skip);

        if let Some(decision) = answer.latch() {
            latched = Some(decision);
        }
        let _ = req.reply.send(answer.resolution());
    }
}

/// Blocking terminal prompt
fn prompt_header(source: &Path, target: &Path) -> String {
    let describe = |p: &Path| match p.metadata() {
        Ok(m) => format!(
            "{:>10}   {}",
            indicatif::HumanBytes(m.len()).to_string(),
            m.modified()
                .map(|t| {
                    // Seconds precision: milliseconds are noise when comparing two files.
                    let s = crate::logger::format_time(t);
                    s.split('.').next().unwrap_or(&s).to_owned()
                })
                .unwrap_or_else(|_| "unknown time".to_owned())
        ),
        Err(e) => format!("unreadable ({e})"),
    };

    format!(
        "
Conflict: {}
  source  {}
  target  {}
Action",
        target.display(),
        describe(source),
        describe(target),
    )
}

fn prompt(source: &Path, target: &Path) -> Answer {
    let header = prompt_header(source, target);

    match dialoguer::Select::new()
        .with_prompt(header)
        .items(&Answer::LABELS)
        .default(1)
        .interact()
    {
        Ok(i) => Answer::from_index(i),
        Err(e) => {
            let msg = format!(
                "Conflict prompt failed for {}: {e}
Stopping rather than silently skipping the                  remaining conflicts. Re-run with an explicit --conflict skip or --conflict                  overwrite.",
                target.display()
            );
            error!("{msg}");
            eprintln!(
                "
error: {msg}"
            );
            Answer::Quit
        }
    }
}
