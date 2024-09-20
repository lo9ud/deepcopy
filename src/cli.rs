use std::path::PathBuf;

use clap::{Parser, ValueHint};

use crate::conflict_resolver::ConflictResolutionStrategy;


#[derive(Parser, Debug)]
#[command(about = "A deep copying utility")]
pub struct Cli {
    #[arg(short, long, value_hint= ValueHint::FilePath)]
    pub source: PathBuf,

    #[arg(short, long, value_hint = ValueHint::FilePath)]
    pub dest: PathBuf,

    #[arg(short = 'c', long = "conflict", default_value = "skip")]
    pub conflict_resolution_strategy: ConflictResolutionStrategy,
}

fn main() {
    let args = Cli::parse();
    println!("{:?}", args);
}