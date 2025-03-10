// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(dead_code)]

use std::collections::HashMap;
use std::fs;
use std::ops::{Bound, Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[cfg(feature = "foreground-iterator")]
use crate::lsm_iterator::foreground_iter::ForegroundIterator;

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(())?;
        self.compaction_notifier.send(())?;
        if let Some(handle) = self.flush_thread.lock().as_ref() {
            while handle.is_finished() {
                println!("Waiting flush thread to be over.");
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
        if let Some(handle) = self.compaction_thread.lock().as_ref() {
            while handle.is_finished() {
                println!("Waiting compact thread to be over.");
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

pub trait IntoBounds<'a> {
    fn into_bounds(self) -> (Bound<&'a [u8]>, Bound<&'a [u8]>);
}

impl<'a> IntoBounds<'a> for Range<&'a [u8]> {
    fn into_bounds(self) -> (Bound<&'a [u8]>, Bound<&'a [u8]>) {
        (Bound::Included(self.start), Bound::Excluded(self.end))
    }
}

impl<'a> IntoBounds<'a> for RangeFrom<&'a [u8]> {
    fn into_bounds(self) -> (Bound<&'a [u8]>, Bound<&'a [u8]>) {
        (Bound::Included(self.start), Bound::Unbounded)
    }
}

impl<'a> IntoBounds<'a> for RangeFull {
    fn into_bounds(self) -> (Bound<&'a [u8]>, Bound<&'a [u8]>) {
        (Bound::Unbounded, Bound::Unbounded)
    }
}

impl<'a> IntoBounds<'a> for RangeTo<&'a [u8]> {
    fn into_bounds(self) -> (Bound<&'a [u8]>, Bound<&'a [u8]>) {
        (Bound::Unbounded, Bound::Excluded(self.end))
    }
}

impl<'a> IntoBounds<'a> for RangeToInclusive<&'a [u8]> {
    fn into_bounds(self) -> (Bound<&'a [u8]>, Bound<&'a [u8]>) {
        (Bound::Unbounded, Bound::Included(self.end))
    }
}

impl<'a> IntoBounds<'a> for RangeInclusive<&'a [u8]> {
    fn into_bounds(self) -> (Bound<&'a [u8]>, Bound<&'a [u8]>) {
        let (start, end) = self.into_inner();
        (Bound::Included(start), Bound::Included(end))
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        if !path.exists() {
            fs::create_dir_all(path)?;
        }

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // Find in the active memtable.
        let res = snapshot.memtable.get(key);
        if let Some(val) = res {
            if !val.is_empty() {
                return Ok(Some(val));
            }
            return Ok(None);
        }

        // Find in imm_memtables.
        for memtable in snapshot.imm_memtables.iter() {
            if let Some(val) = memtable.get(key) {
                if !val.is_empty() {
                    return Ok(Some(val));
                }
                return Ok(None);
            }
        }

        // Find in SsTables.
        for sst_id in snapshot.l0_sstables.iter() {
            if let Some(sst) = snapshot.sstables.get(sst_id) {
                if !sst.key_within(KeySlice::from_slice(key)) {
                    continue;
                }

                let iter = SsTableIterator::create_and_seek_to_key(
                    Arc::clone(sst),
                    KeySlice::from_slice(key),
                )?;
                if iter.is_valid() && key.eq(iter.key().raw_ref()) {
                    if iter.value().is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(Bytes::copy_from_slice(iter.value())));
                }
            } else {
                return Err(anyhow::Error::msg("sst not found in storage state"));
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let r = self.state.read();
        r.memtable.put(key, value)?;
        if r.memtable.approximate_size() >= self.options.target_sst_size {
            let state_guard = self.state_lock.lock();
            drop(r);
            self.force_freeze_memtable(&state_guard)?;
        }
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable.
    ///
    /// Note that calling this function is with state_lock held.
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let mut guard = self.state.write();
        let mut snapshot = guard.as_ref().clone();
        let new_memtable = MemTable::create(self.next_sst_id());
        let old_memtable = std::mem::replace(&mut snapshot.memtable, Arc::new(new_memtable));
        snapshot.imm_memtables.insert(0, old_memtable);
        *guard = Arc::new(snapshot);
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();
        let mut snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };

        if let Some(to_flush) = snapshot.imm_memtables.pop() {
            let builder = SsTableBuilder::new(self.options.block_size);
            let path = self.path.join(format!("{}.sst", to_flush.id()));
            let sst = to_flush.flush(builder, Some(Arc::clone(&self.block_cache)), path)?;

            snapshot.l0_sstables.insert(0, sst.sst_id());
            snapshot.sstables.insert(sst.sst_id(), Arc::new(sst));

            let mut guard = self.state.write();
            *guard = Arc::new(snapshot);
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let imm_memtables = snapshot.imm_memtables.clone();
        let mut mem_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);

        // Prepare iters.
        mem_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        for imm in imm_memtables {
            mem_iters.push(Box::new(imm.scan(lower, upper)));
        }

        let merge_mem_iter = MergeIterator::create(mem_iters);

        // Now SsTableIters. (Currently only L0)
        let mut sst_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for sst_id in snapshot.l0_sstables.iter() {
            let iter = if let Some(sst) = snapshot.sstables.get(sst_id) {
                // Skip if sst is not needed.
                if !sst.range_overlaps(lower, upper) {
                    continue;
                }

                match lower {
                    Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                        Arc::clone(sst),
                        KeySlice::from_slice(key),
                    )?,
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            Arc::clone(sst),
                            KeySlice::from_slice(key),
                        )?;
                        if iter.key().raw_ref().eq(key) {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(Arc::clone(sst))?,
                }
            } else {
                return Err(anyhow::Error::msg(
                    "target sst does not exist in storage state",
                ));
            };
            sst_iters.push(Box::new(iter));
        }

        let merge_sst_iter = MergeIterator::create(sst_iters);

        let two_merge_iter = TwoMergeIterator::create(merge_mem_iter, merge_sst_iter)?;

        let lsm_iter = LsmIterator::new(two_merge_iter, map_bound(upper))?;

        Ok(FusedIterator::new(lsm_iter))
    }

    pub fn scan_rg<'a>(&self, rg: impl IntoBounds<'a>) -> Result<FusedIterator<LsmIterator>> {
        let (lower, upper) = rg.into_bounds();
        self.scan(lower, upper)
    }

    #[allow(dead_code)]
    #[cfg(feature = "foreground-iterator")]
    pub fn scan_foreguard(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<ForegroundIterator<LsmIterator>> {
        let period = std::time::Duration::from_secs(3);

        let guard = self.state.read();

        let snapshot = guard.imm_memtables.clone();
        let mut iters = Vec::with_capacity(guard.imm_memtables.len() + 1);

        // Prepare iters.
        iters.push(Box::new(guard.memtable.scan(lower, upper)));
        for imm in snapshot {
            iters.push(Box::new(imm.scan(lower, upper)));
        }

        let merge_iter = MergeIterator::create(iters);
        let lsm_iter = LsmIterator::new(merge_iter)?;
        Ok(ForegroundIterator::new(
            Arc::clone(&self.state),
            period,
            lsm_iter,
            map_bound(upper),
        ))
    }

    pub fn list_all_items(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) {
        let mut iter = self.scan(lower, upper).unwrap();
        loop {
            println!(
                "{:?}, {:?}",
                Bytes::copy_from_slice(iter.key()),
                Bytes::copy_from_slice(iter.value())
            );
            iter.next().unwrap();
            if !iter.is_valid() {
                break;
            }
        }
    }
}
