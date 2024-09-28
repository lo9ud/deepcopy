use std::path::PathBuf;

use flexi_logger::{FileSpec, Logger, LoggerHandle};

pub static LOG_FILE: std::sync::LazyLock<PathBuf> = std::sync::LazyLock::new(|| std::env::temp_dir().join("deepcopy.log"));


pub fn get_logger() -> Result<LoggerHandle, flexi_logger::FlexiLoggerError> {
    Logger::try_with_env_or_str("info")?
        .log_to_file(FileSpec::try_from(LOG_FILE.clone())?)
        .format(log_file_fmt)
        .start()
}

fn log_file_fmt(
    write: &mut dyn std::io::Write,
    now: &mut flexi_logger::DeferredNow,
    record: &log::Record,
 ) -> std::io::Result<()> {
    write!(write, "[{}] [{:<6}] | {}",
        now.now().format("%Y-%m-%d %H:%M:%S%.3f"),
        record.level(),
        record.args()
    )
 }