//! Minimal file logger behind the `log` facade.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use log::{LevelFilter, Log, Metadata, Record};

pub static LOG_FILE: std::sync::LazyLock<PathBuf> =
    std::sync::LazyLock::new(|| std::env::temp_dir().join("deepcopy.log"));

struct FileLogger {
    sink: Mutex<BufWriter<File>>,
    level: LevelFilter,
}

impl Log for FileLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let Ok(mut sink) = self.sink.lock() else {
            return; // A poisoned lock must not take the whole copy down.
        };
        let _ = writeln!(
            sink,
            "[{}] [{:<6}] | {}",
            timestamp(),
            record.level(),
            record.args()
        );
    }

    fn flush(&self) {
        if let Ok(mut sink) = self.sink.lock() {
            let _ = sink.flush();
        }
    }
}

/// `YYYY-MM-DD HH:MM:SS.mmm` - UTC.
fn timestamp() -> String {
    format_time(SystemTime::now())
}

/// `YYYY-MM-DD HH:MM:SS.mmm` for an arbitrary instant. Shared with the conflict prompt, which
/// shows both files' modification times.
pub fn format_time(t: SystemTime) -> String {
    let now = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let millis = now.subsec_millis();
    let secs = now.as_secs();

    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (y, mo, d) = civil_from_days(days as i64);

    format!("{y:04}-{mo:02}-{d:02} {hh:02}:{mm:02}:{ss:02}.{millis:03}")
}

/// Howard Hinnant's `civil_from_days`: days since 1970-01-01 to (year, month, day).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Installs the global logger. Returns an error if the log file cannot be opened, so the caller
/// can warn rather than silently running with no logging.
pub fn init(debug: bool) -> Result<(), String> {
    let level = if debug {
        LevelFilter::Debug
    } else {
        LevelFilter::Info
    };

    let file = File::create(&*LOG_FILE)
        .map_err(|e| format!("could not open {}: {e}", LOG_FILE.display()))?;

    let logger = FileLogger {
        sink: Mutex::new(BufWriter::new(file)),
        level,
    };

    log::set_boxed_logger(Box::new(logger)).map_err(|e| e.to_string())?;
    log::set_max_level(level);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::civil_from_days;

    #[test]
    fn epoch_and_known_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(19_723), (2024, 1, 1)); // leap year boundary
        assert_eq!(civil_from_days(20_608), (2026, 6, 4));
    }
}
