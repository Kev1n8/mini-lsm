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

use std::ops::Bound;

use anyhow::Result;
use bytes::Bytes;

use crate::{
    iterators::{
        merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    upper: Bound<Bytes>,
    ended: bool,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper: Bound<Bytes>) -> Result<Self> {
        // The iter could start with tombstones or simply invalid.
        let mut iter = iter;
        while iter.is_valid() && iter.value().is_empty() {
            iter.next()?;
        }
        let ended = !iter.is_valid();
        Ok(Self {
            inner: iter,
            upper,
            ended,
        })
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        !self.ended && self.inner.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    /// Next till not tombstone and stop at end.
    fn next(&mut self) -> Result<()> {
        if self.ended {
            return Ok(());
        }
        self.inner.next()?;

        if !self.inner.is_valid() {
            self.ended = true;
        }

        // Check end.
        match self.upper.as_ref() {
            Bound::Included(end) => {
                if self.key().gt(end.as_ref()) {
                    self.ended = true;
                }
            }
            Bound::Excluded(end) => {
                if self.key().ge(end.as_ref()) {
                    self.ended = true;
                }
            }
            Bound::Unbounded => {}
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.has_errored {
            unreachable!() // Should never called when error has happened.
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if self.has_errored {
            unreachable!() // Should never called when error has happened.
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            Err(anyhow::Error::msg("calling error FusedIterator"))
        } else {
            if self.iter.is_valid() {
                if let e @ Err(_) = self.iter.next() {
                    self.has_errored = true;
                    return e;
                }
            }
            Ok(())
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}

#[cfg(feature = "foreground-iterator")]
pub mod foreground_iter {
    use crate::lsm_storage::LsmStorageState;
    use parking_lot::RwLock;
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use super::*;

    type SharedState = Arc<RwLock<Arc<LsmStorageState>>>;

    /// `ForegroundIterator` is for user who wants to hold iterator for a long time.
    ///
    /// Every call on `next`, it checks if the last time of updating elapses the peroid setted,
    /// and drop the iter to release memory.
    pub struct ForegroundIterator<I: StorageIterator> {
        state: SharedState,
        iter: FusedIterator<I>,
        last_update: Instant, // Last time recreating iter.
        period: Duration,     // For how long to recreate iter.
        upper: Bound<Bytes>,  // Record the upper bound for recreating.
    }

    impl ForegroundIterator<LsmIterator> {
        pub fn new(
            state: SharedState,
            period: Duration,
            iter: LsmIterator,
            upper: Bound<Bytes>,
        ) -> Self {
            Self {
                state,
                iter: FusedIterator::new(iter),
                last_update: Instant::now(),
                period,
                upper,
            }
        }

        fn check_update(&mut self) {
            if self.last_update.elapsed().ge(&self.period) {
                let guard = self.state.read();

                let (lower, upper) = (
                    Bound::Excluded(self.key()),
                    // Safety: The lifetime of the key is guranteed to be at least as long as this iter.
                    // And they will be useless after a simple copy.
                    // Bound::Excluded(unsafe {
                    //     let key = self.iter.key();
                    //     let ptr = &key as *const _ as *const u8;
                    //     let len = std::mem::size_of_val(&key);
                    //     slice::from_raw_parts(ptr, len)
                    // }),
                    match self.upper.as_ref() {
                        Bound::Excluded(b) => Bound::Excluded(b.as_ref()),
                        Bound::Included(b) => Bound::Included(b.as_ref()),
                        Bound::Unbounded => Bound::Unbounded,
                    },
                );

                guard.memtable.scan(lower, upper);

                let snapshot = guard.imm_memtables.clone();
                let mut iters = Vec::with_capacity(guard.imm_memtables.len() + 1);

                // Prepare iters.
                iters.push(Box::new(guard.memtable.scan(lower, upper)));
                for imm in snapshot {
                    iters.push(Box::new(imm.scan(lower, upper)));
                }

                let merge_mem_iter = MergeIterator::create(iters);

                //

                let lsm_iter = if let Ok(iter) = LsmIterator::new(merge_iter) {
                    // If success, we continue process.
                    iter
                } else {
                    // Else we give up this time of update.
                    return;
                };

                let _ = std::mem::replace(&mut self.iter, FusedIterator::new(lsm_iter));
                self.last_update = Instant::now();
            }
        }
    }

    impl StorageIterator for ForegroundIterator<LsmIterator> {
        type KeyType<'a>
        = <LsmIterator as StorageIterator>::KeyType<'a>
    where
        Self: 'a;

        fn value(&self) -> &[u8] {
            self.iter.value()
        }

        fn key(&self) -> Self::KeyType<'_> {
            self.iter.key()
        }

        fn is_valid(&self) -> bool {
            self.iter.iter.is_valid()
        }

        fn next(&mut self) -> Result<()> {
            self.check_update();
            self.iter.next()
        }
    }
}
