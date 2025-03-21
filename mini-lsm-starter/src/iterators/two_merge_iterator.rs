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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    at_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    /// Create a TwoMergeIterator.
    ///
    /// Get the iter ready.
    pub fn create(a: A, b: B) -> Result<Self> {
        let (a, mut b) = (a, b);

        let at_a = match (a.is_valid(), b.is_valid()) {
            (true, true) => {
                if a.key() <= b.key() {
                    if a.key() == b.key() {
                        b.next()?;
                    }
                    true
                } else {
                    false
                }
            }
            (true, false) => true,
            (false, true) => false,
            (false, false) => false,
        };
        let mut iter = Self { a, b, at_a };

        if iter.is_valid() && iter.value().is_empty() {
            iter.next()?;
        }

        Ok(iter)
    }

    fn next_inner(&mut self) -> Result<()> {
        match (self.a.is_valid(), self.b.is_valid()) {
            (true, true) => {
                // First discuss which iter is on.
                if self.at_a {
                    // Then make sure the other iter has different key.
                    while self.b.is_valid() && self.a.key() == self.b.key() {
                        self.b.next()?;
                    }
                    // Now update iter on.
                    self.a.next()?;
                    // Check if it's necessary to "swap".
                    if !self.a.is_valid() || (self.b.is_valid() && self.b.key() < self.a.key()) {
                        self.at_a = false;
                    }
                    // If a is invalid after `next`, does not matter.
                    Ok(())
                } else {
                    while self.a.is_valid() && self.a.key() == self.b.key() {
                        self.a.next()?;
                    }
                    self.b.next()?;
                    // If the key is equal, we use self.a.
                    if !self.b.is_valid() || (self.a.is_valid() && self.a.key() <= self.b.key()) {
                        self.at_a = true;
                    }
                    Ok(())
                }
            }
            (true, false) => {
                self.at_a = true;
                self.a.next()
            }
            (false, true) => {
                self.at_a = false;
                self.b.next()
            }
            (false, false) => Ok(()), // Do nothing.
        }
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.at_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.at_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.at_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    /// Next till not tombstone.
    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }
        loop {
            self.next_inner()?;
            if !self.is_valid() || !self.value().is_empty() {
                break;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
