use core::time;
use std::{
    path::PathBuf,
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex,
    },
};

use crate::{conflict_resolver::ConflictResolutionStrategy, ProgressHolder, Stats};

#[derive(Debug, Clone)]
pub struct CopyJob {
    source: PathBuf,
    destination: PathBuf,
}

impl CopyJob {
    pub fn new(source: PathBuf, destination: PathBuf) -> Self {
        Self {
            source,
            destination,
        }
    }

    pub fn _execute(
        &self,
        conflict: ConflictResolutionStrategy,
        mpb: Arc<ProgressHolder>,
        stats: Arc<Mutex<Stats>>,
    ) {
        let file_bytes = std::fs::metadata(&self.source)
            .expect(&format!(
                "Failed to get metadata for {}",
                self.source.display()
            ))
            .len();

        let pb = mpb.add(file_bytes, format!("{}", self.source.display()));

        if !self
            .destination
            .parent()
            .expect("Failed to get parent")
            .exists()
        {
            std::fs::create_dir_all(
                &self
                    .destination
                    .parent()
                    .expect("Failed to create directory"),
            )
            .expect("Failed to create directory");
        }

        if self.destination.exists() {
            match conflict.resolve(self.destination.clone()) {
                crate::conflict_resolver::ConflictResolution::Overwrite => {
                    std::fs::copy(&self.source, &self.destination).expect("Failed to copy file");
                    stats.lock().unwrap().files_overwritten += 1;
                    stats.lock().unwrap().bytes_overwritten += file_bytes;
                }
                crate::conflict_resolver::ConflictResolution::Skip => {
                    stats.lock().unwrap().files_skipped += 1;
                    stats.lock().unwrap().bytes_skipped += file_bytes;
                }
            }
        } else {
            std::fs::copy(&self.source, &self.destination).expect("Failed to copy file");
            stats.lock().unwrap().files += 1;
            stats.lock().unwrap().bytes += file_bytes;
        }
        pb.finish_and_clear();
        mpb.inc_main();
        mpb.remove(&pb);
    }
}

pub struct JobQueue {
    target_root: PathBuf,
    source_root: PathBuf,
    pub jobs: u32,
    sender: Sender<CopyJob>,
    receiver: Receiver<CopyJob>,
}

impl JobQueue {
    pub fn new(source: PathBuf, target: PathBuf) -> Self {
        let (sender, receiver) = channel();
        Self {
            source_root: source,
            target_root: target,
            jobs: 0,
            sender,
            receiver,
        }
    }

    pub fn populate(&mut self) {
        walkdir::WalkDir::new(&self.source_root)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .skip(1) //Skip the root directory
            .for_each(|entry| {
                let source = entry.path();
                if source.is_dir() {
                    return;
                }
                let destination = self.target_root.join(
                    source
                        .strip_prefix(&self.source_root)
                        .expect("Failed to strip prefix"),
                );
                let job = CopyJob::new(source.into(), destination.into());
                self.push(job).expect("Failed to push job");
            });
    }

    fn push(&mut self, job: CopyJob) -> Result<(), std::sync::mpsc::SendError<CopyJob>> {
        self.sender.send(job)?;
        self.jobs += 1;
        Ok(())
    }

    pub fn pop(&mut self) -> Result<CopyJob, std::sync::mpsc::RecvTimeoutError> {
        match self.receiver.recv_timeout(time::Duration::from_secs(1)) {
            Ok(job) => {
                self.jobs -= 1;
                Ok(job)
            }
            Err(e) => Err(e),
        }
    }
}

impl Iterator for JobQueue {
    type Item = CopyJob;

    fn next(&mut self) -> Option<Self::Item> {
        self.pop().ok()
    }
}
