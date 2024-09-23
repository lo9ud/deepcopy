use std::path::PathBuf;

use clap::{value_parser, Parser, ValueHint};

use crate::conflict_resolver::ConflictResolutionStrategy;


#[derive(Parser, Debug)]
#[command(about = env!("CARGO_PKG_DESCRIPTION"), version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), long_about = "This tool is designed to be idempotent, such that it can be interrrupted repeatedly and rerun without damage to files or data.")]
pub struct Cli {
    #[arg(short, long, value_hint= ValueHint::FilePath, value_parser = value_parser!(PathBuf))]
    pub source: PathBuf,

    #[arg(short, long, value_hint = ValueHint::FilePath, value_parser = value_parser!(PathBuf))]
    pub dest: PathBuf,

    #[arg(short = 'c', long = "conflict", default_value = "skip")]
    pub conflict_resolution_strategy: ConflictResolutionStrategy,

    #[arg(long, hide = true, default_value = "false")]
    pub debug: bool,

    #[arg(long, default_value = "false")]
    pub dry_run: bool,
}