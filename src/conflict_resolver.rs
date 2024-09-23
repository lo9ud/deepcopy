use std::path::PathBuf;

use clap::ValueEnum;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ConflictResolutionStrategy {
    // TODO: Implement the variants for the ConflictResolution enum
    //  - Oldest
    //  - Newest
    //  - Largest
    //  - Smallest
    ChecksumOverwrite, //  -  (make default)
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

fn sha256_digest(file: &PathBuf) -> String {
    use sha2::{Digest, Sha256};
    use std::fs::File;
    use std::io::Read;

    let mut file = File::open(file).expect("Failed to open file");
    let mut hasher = Sha256::new();
    let mut buffer = [0; 1024];
    loop {
        let n = file.read(&mut buffer).expect("Failed to read file");
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    format!("{:x}", hasher.finalize())
}

impl ConflictResolutionStrategy {
    pub fn resolve(&self, _source:PathBuf, _target:PathBuf) -> ConflictResolution {
        match self {
            Self::Overwrite => ConflictResolution::Overwrite,
            Self::Skip => ConflictResolution::Skip,
            Self::ChecksumOverwrite => {
                // check len
                if std::fs::metadata(&_source).expect("Failed to get metadata").len() != std::fs::metadata(&_target).expect("Failed to get metadata").len() {
                    return ConflictResolution::Overwrite;
                }
                // check sha256
                if sha256_digest(&_source) == sha256_digest(&_target) {
                    ConflictResolution::Skip
                } else {
                    ConflictResolution::Overwrite
                }
            },

        	}
    }
}

