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

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    /// Load all iters into heap.
    ///
    /// Ignore invalid iters. If all is invalid, push the oldest iter.
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        let mut iters = iters;
        let mut heap = BinaryHeap::<HeapWrapper<I>>::new();
        // Make sure there's at least 1 iter in the heap, for `pop()` below.
        if iters.iter().all(|iter| !iter.is_valid()) {
            return Self {
                iters: heap,
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            };
        }

        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        let current = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(current),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|cur| cur.1.is_valid())
            .unwrap_or(false)
    }

    /// Update state of MergeIterator to its next.
    ///
    /// Steps:
    /// 1. Call `heap.top.next()` until the key is different or all iters in heap are popped.
    ///    It could happen that heap.top is invalid, should pop.
    /// 2. Call `current.next()`.
    /// 3. Compare the key between current and top, swap if necessary.
    fn next(&mut self) -> Result<()> {
        // Guaranteed that next is called with present current.
        let current = self.current.as_mut().unwrap();
        while let Some(mut top) = self.iters.peek_mut() {
            if current.1.key().eq(&top.1.key()) {
                if let e @ Err(_) = top.1.next() {
                    PeekMut::pop(top);
                    return e;
                }

                if !top.1.is_valid() {
                    PeekMut::pop(top);
                }
            } else {
                break;
            }
        }

        // Now the `top` is either `None` or it has a different key compared with `current`.
        current.1.next()?; // Is this a good practice? Maybe need to handle it.

        // `current` could be invalid.
        if !current.1.is_valid() {
            if let Some(top) = self.iters.pop() {
                *current = top;
            }
            return Ok(());
        }

        // May need to swap if `top` has smaller key.
        if let Some(mut top) = self.iters.peek_mut() {
            if *current < *top {
                // Note that we are trying to put the "larger" item in front.
                std::mem::swap(current, &mut top);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters.len()
            + self
                .current
                .as_ref()
                .map(|wrapper| if wrapper.1.is_valid() { 1 } else { 0 })
                .unwrap_or(0)
    }
}
