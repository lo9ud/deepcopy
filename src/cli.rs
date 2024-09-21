use std::path::PathBuf;

use clap::{value_parser, Parser, ValueHint};

use crate::conflict_resolver::ConflictResolutionStrategy;


#[derive(Parser, Debug)]
#[command(about = "A deep copying utility")]
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