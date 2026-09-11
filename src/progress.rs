//! The single progress bar.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use indicatif::{HumanBytes, ProgressBar, ProgressStyle};

/// Groups digits so six-figure file counts stay readable.
fn commas(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// Minimum columns a bar needs before it drawn at all.
const MIN_BAR_COLS: u16 = 10;

/// How often the non-terminal fallback emits a line.
const PLAIN_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Tier {
    Full,
    Medium,
    Narrow,
    Plain,
}

impl Tier {
    fn for_width(cols: Option<u16>) -> Self {
        // A failed size query means output is redirected or there is no terminal.
        let Some(cols) = cols else {
            return Tier::Plain;
        };
        match cols {
            c if c >= 100 => Tier::Full,
            c if c >= 60 => Tier::Medium,
            // Below this the fixed segments alone would overflow, so a wide bar cannot help.
            c if c >= 40 + MIN_BAR_COLS => Tier::Narrow,
            _ => Tier::Plain,
        }
    }

    fn template(self, scanning: bool) -> &'static str {
        if scanning {
            return match self {
                Tier::Full => "{spinner:.green} [{elapsed_precise}] scanning… {msg}",
                Tier::Medium => "{spinner:.green} scanning… {msg}",
                Tier::Narrow | Tier::Plain => "{spinner:.green} {msg}",
            };
        }
        match self {
            Tier::Full => {
                "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}]                  {bytes}/{total_bytes} ({percent}%) · {msg}"
            }
            Tier::Medium => {
                "{spinner:.green} [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} · {msg}"
            }
            Tier::Narrow => "{spinner:.green} {wide_bar:.cyan/blue} {percent}%",
            Tier::Plain => "{spinner:.green} {bytes}/{total_bytes} ({percent}%)",
        }
    }

    /// The scanning phase puts its counts in the message for every tier, so it is always shown
    /// then; only the finished bar drops it on the narrow tiers.
    fn shows_message(self, scanning: bool) -> bool {
        scanning || matches!(self, Tier::Full | Tier::Medium)
    }

    fn shows_cloud_detail(self) -> bool {
        self == Tier::Full
    }
}

/// How many updates to coalesce before rebuilding the message string.
const MESSAGE_REFRESH_EVERY: u64 = 64;

struct Counters {
    files_done: AtomicU64,
    files_found: AtomicU64,
    cloud_found: AtomicU64,
    scanning: AtomicBool,
    updates: AtomicU64,
}

/// Cloneable handle to the run's progress display.
#[derive(Clone)]
pub struct Progress {
    bar: ProgressBar,
    counters: Arc<Counters>,
    tier: Tier,
    /// No bar is drawn at all; the caller emits periodic plain lines instead.
    plain: bool,
}

impl Progress {
    /// `force_plain` comes from `--no-tty`
    pub fn new(force_plain: bool) -> Self {
        let cols = console::Term::stdout().size_checked().map(|(_, c)| c);

        let plain = force_plain || cols.is_none();
        let tier = Tier::for_width(cols);

        let bar = if plain {
            ProgressBar::with_draw_target(Some(0), indicatif::ProgressDrawTarget::hidden())
        } else {
            // Start at zero length: discovery streams, so the denominator grows as files are found.
            let bar = ProgressBar::new(0);
            bar.set_style(
                ProgressStyle::default_bar()
                    .progress_chars("█▉▊▋▌▍▎▏ ")
                    .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
                    .template(tier.template(true))
                    .expect("built-in template should be valid"),
            );
            bar.enable_steady_tick(Duration::from_millis(120));
            bar
        };

        Self {
            bar,
            counters: Arc::new(Counters {
                files_done: AtomicU64::new(0),
                files_found: AtomicU64::new(0),
                cloud_found: AtomicU64::new(0),
                scanning: AtomicBool::new(true),
                updates: AtomicU64::new(0),
            }),
            tier,
            plain,
        }
    }

    /// Whether the bar is actually being drawn.
    pub fn is_drawing(&self) -> bool {
        !self.plain
    }

    pub fn plain_interval(&self) -> Duration {
        PLAIN_INTERVAL
    }

    /// Called by the walker for each discovered file, growing the denominator.
    pub fn found(&self, bytes: u64, dehydrated: bool) {
        self.counters.files_found.fetch_add(1, Ordering::Relaxed);
        if dehydrated {
            self.counters.cloud_found.fetch_add(1, Ordering::Relaxed);
        }
        self.bar.inc_length(bytes);
        self.refresh_message();
    }

    /// Called once the walk finishes, so the message can drop its "scanning" suffix.
    pub fn scan_complete(&self) {
        self.counters.scanning.store(false, Ordering::Relaxed);
        if !self.plain {
            if let Ok(style) = ProgressStyle::default_bar()
                .progress_chars("█▉▊▋▌▍▎▏ ")
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
                .template(self.tier.template(false))
            {
                self.bar.set_style(style);
            }
        }
        self.refresh_message_now();
    }

    /// Called when a file is finished, whether copied, skipped or failed.
    pub fn finished(&self, bytes: u64) {
        self.counters.files_done.fetch_add(1, Ordering::Relaxed);
        self.bar.inc(bytes);
        self.refresh_message();
    }

    fn refresh_message(&self) {
        if !self
            .tier
            .shows_message(self.counters.scanning.load(Ordering::Relaxed))
        {
            return;
        }
        let n = self.counters.updates.fetch_add(1, Ordering::Relaxed);
        if n % MESSAGE_REFRESH_EVERY == 0 {
            self.bar.set_message(self.message());
        }
    }

    /// Forces the message up to date, for the end of a phase.
    fn refresh_message_now(&self) {
        if self
            .tier
            .shows_message(self.counters.scanning.load(Ordering::Relaxed))
        {
            self.bar.set_message(self.message());
        }
    }

    fn message(&self) -> String {
        let done = self.counters.files_done.load(Ordering::Relaxed);
        let found = self.counters.files_found.load(Ordering::Relaxed);
        let cloud = self.counters.cloud_found.load(Ordering::Relaxed);
        let scanning = self.counters.scanning.load(Ordering::Relaxed);

        let mut msg = if scanning {
            format!(
                "{} found · {} copied · {}",
                commas(found),
                commas(done),
                HumanBytes(self.bar.position())
            )
        } else {
            format!("{}/{} files", commas(done), commas(found))
        };

        if self.tier.shows_cloud_detail() && cloud > 0 {
            msg.push_str(&format!(" · {} online-only", commas(cloud)));
        }
        msg
    }

    /// A one-line status for non-terminal output.
    pub fn plain_line(&self) -> String {
        let done = self.counters.files_done.load(Ordering::Relaxed);
        let found = self.counters.files_found.load(Ordering::Relaxed);
        let scanning = self.counters.scanning.load(Ordering::Relaxed);
        format!(
            "{}/{} files · {} / {}{}",
            done,
            found,
            HumanBytes(self.bar.position()),
            HumanBytes(self.bar.length().unwrap_or(0)),
            if scanning { " (scanning)" } else { "" },
        )
    }

    /// Runs `f` with the bar temporarily cleared, so a prompt is not drawn over.
    pub fn suspend<F: FnOnce() -> R, R>(&self, f: F) -> R {
        self.bar.suspend(f)
    }

    /// Prints a line above the bar without corrupting it.
    ///
    /// `ProgressBar::println` is a no-op on a hidden bar, so a plain run must fall back to
    /// `println!` -- otherwise messages like the log path and the out-of-space warning vanish.
    pub fn println(&self, line: impl AsRef<str>) {
        if self.plain {
            println!("{}", line.as_ref());
        } else {
            self.bar.println(line);
        }
    }

    pub fn finish_and_clear(&self) {
        self.bar.finish_and_clear();
    }
}

impl Default for Progress {
    fn default() -> Self {
        Self::new(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiers_follow_width() {
        assert_eq!(Tier::for_width(Some(120)), Tier::Full);
        assert_eq!(Tier::for_width(Some(100)), Tier::Full);
        assert_eq!(Tier::for_width(Some(99)), Tier::Medium);
        assert_eq!(Tier::for_width(Some(60)), Tier::Medium);
        assert_eq!(Tier::for_width(Some(59)), Tier::Narrow);
        assert_eq!(Tier::for_width(Some(50)), Tier::Narrow);
        assert_eq!(Tier::for_width(Some(30)), Tier::Plain);
    }

    #[test]
    fn every_template_parses() {
        for tier in [Tier::Full, Tier::Medium, Tier::Narrow, Tier::Plain] {
            ProgressStyle::default_bar()
                .template(tier.template(true))
                .unwrap_or_else(|e| panic!("{tier:?} template invalid: {e}"));
        }
    }
}
