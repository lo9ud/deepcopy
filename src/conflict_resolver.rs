use std::path::PathBuf;

use clap::ValueEnum;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ConflictResolutionStrategy {
    Overwrite,
    Skip,
}

impl Default for ConflictResolutionStrategy {
    fn default() -> Self {
        Self::Overwrite
    }
}

pub enum ConflictResolution {
    Overwrite,
    Skip,
}

impl ConflictResolutionStrategy {
    pub fn resolve(&self, _target:PathBuf) -> ConflictResolution {
        match self {
            Self::Overwrite => ConflictResolution::Overwrite,
            Self::Skip => ConflictResolution::Skip,
        	}
    }
}

