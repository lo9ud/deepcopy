use std::path::PathBuf;

use clap::ValueEnum;
use log::warn;

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
    pub fn resolve(&self, source:PathBuf, target:PathBuf) -> ConflictResolution {
        match self._resolve(source.clone(), target.clone()) {
            Ok(resolution) => resolution,
            Err(e) => {
                warn!("Failed to resolve conflict for {} -> {}, skipping instead. Reason: {}", source.display(), target.display(), e);
                ConflictResolution::Skip
            }
        }
    }

    fn _resolve(&self, source:PathBuf, target:PathBuf) -> Result<ConflictResolution, std::io::Error> {
        match self {
            Self::Overwrite => Ok(ConflictResolution::Overwrite),
            Self::Skip => Ok(ConflictResolution::Skip),
            Self::ChecksumOverwrite => {
                // check len
                if std::fs::metadata(&source)?.len() != std::fs::metadata(&target)?.len() {
                    return Ok(ConflictResolution::Overwrite)
                }
                // check sha256
                if sha256_digest(&source) == sha256_digest(&target) {
                    Ok(ConflictResolution::Skip)
                } else {
                    Ok(ConflictResolution::Overwrite)
                }
            },
        }
    }
}

