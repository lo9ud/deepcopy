use core::time;
use std::{
    path::PathBuf, sync::{
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex,
    }
};

use log::{debug, info, warn};

use crate::{conflict_resolver::ConflictResolutionStrategy, ProgressHolder, Stats};

#[derive(Debug, Clone)]
pub struct CopyJob {
    source: PathBuf,
    target: PathBuf,
}

impl CopyJob {
    pub fn new(source: PathBuf, destination: PathBuf) -> Self {
        Self {
            source,
            target: destination,
        }
    }

    pub fn execute(
        &self,
        conflict: ConflictResolutionStrategy,
        mpb: Arc<ProgressHolder>,
        stats: Arc<Mutex<Stats>>,
        dry_run:bool,
    ) {
        debug!("Copying {} -> {}", self.source.display(), self.target.display());
        match self._execute(conflict, mpb, stats, dry_run) {
            Ok(_) => {}
            Err(e) => {
                warn!("Failed to copy {} -> {}: {}", self.source.display(), self.target.display(), e);
            }
        }
    }

    fn _execute(
        &self,
        conflict: ConflictResolutionStrategy,
        mpb: Arc<ProgressHolder>,
        stats: Arc<Mutex<Stats>>,
        dry_run:bool,
    ) -> Result<(), std::io::Error> {
        let file_bytes = std::fs::metadata(&self.source)
            .expect(&format!(
                "Failed to get metadata for {}",
                self.source.display()
            ))
            .len();

        let to_from = self.source.components().into_iter()
        .rev().zip(self.target.components()
        .into_iter()
        .rev())
        .collect::<Vec<_>>();
            
        let (to, _) = to_from.iter()
        .take_while(|(to, from)| to == from)
        .cloned()
        .collect::<Vec<_>>()
        .iter().rev().cloned()
        .unzip::<_, _, PathBuf, PathBuf>();
        

        let pb = mpb.add(file_bytes, format!("{}{}", if dry_run {"DRY RUN: "} else {""}, to.display()));

        if !self
            .target
            .parent()
            .expect("Failed to get parent")
            .exists()
        {
            std::fs::create_dir_all(
                &self
                    .target
                    .parent().ok_or(std::io::Error::from(std::io::ErrorKind::NotFound))?,
            )?;
        }

        if self.target.exists() {
            match conflict.resolve(self.source.clone(), self.target.clone()) {
                crate::conflict_resolver::ConflictResolution::Overwrite => {
                    std::fs::copy(&self.source, &self.target)?;
                    stats.lock().unwrap().files_overwritten += 1;
                    stats.lock().unwrap().bytes_overwritten += file_bytes;
                }
                crate::conflict_resolver::ConflictResolution::Skip => {
                    stats.lock().unwrap().files_skipped += 1;
                    stats.lock().unwrap().bytes_skipped += file_bytes;
                }
            }
        } else {
            if !dry_run {
                std::fs::copy(&self.source, &self.target)?;
            } else {
                std::thread::sleep(time::Duration::from_secs(3));
            }
            stats.lock().unwrap().files += 1;
            stats.lock().unwrap().bytes += file_bytes;
        }
        pb.finish_and_clear();
        mpb.inc_main();
        mpb.remove(&pb);
        Ok(())
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

    pub fn populate(&mut self) -> Result<(), std::io::Error>  {
        self._populate()
    }

    fn _populate(&mut self) -> Result<(), std::io::Error> {
        walkdir::WalkDir::new(&self.source_root)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .skip(1) //Skip the root directory
            .try_for_each(|entry| -> Result<(), std::io::Error> {
                let source = entry.path();
                if source.is_dir() {
                    return Ok(());
                }
                let destination = self.target_root.join(
                    source
                        .strip_prefix(&self.source_root)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::NotFound, e))?
                );
                let job = CopyJob::new(source.into(), destination.into());
                self.push(job).expect("Failed to push job");
                Ok(())
            })
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
