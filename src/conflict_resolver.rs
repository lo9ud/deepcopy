use std::path::PathBuf;

use clap::ValueEnum;
use dialoguer::Confirm;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ConflictResolutionStrategy {
    Overwrite,
    Skip,
}

impl Default for ConflictResolutionStrategy {
    fn default() -> Self {
        Self::overwrite()
    }
}

pub enum ConflictResolution {
    Overwrite,
    Skip,
}

impl ConflictResolutionStrategy {
    pub fn overwrite() -> Self {
        Self::Overwrite
    }

    pub fn skip() -> Self {
        Self::Skip
    }

    pub fn resolve(&self, target:PathBuf) -> ConflictResolution {
        match self {
            Self::Overwrite => ConflictResolution::Overwrite,
            Self::Skip => ConflictResolution::Skip,
        	}
    }
}

