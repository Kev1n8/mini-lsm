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

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        debug_assert!(!block.offsets.is_empty(), "sstable block is empty");

        let (key_st, key_ed) = parse_range(&block.data[..]);
        let value_range = parse_range(&block.data[key_ed..]);

        let first_key = block.data[key_st..key_ed].to_vec();

        Self {
            block,
            key: KeyVec::from_vec(first_key.clone()),
            value_range,
            idx: 0,
            first_key: KeyVec::from_vec(first_key),
        }
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        debug_assert!(!block.offsets.is_empty(), "sstable block is empty");

        let (idx, offset) = block.find_offset(key.raw_ref());

        let (key_st, key_ed) = parse_range(&block.data[offset as usize..]);
        let value_range = parse_range(&block.data[key_ed..]);

        let key_raw = block.data[key_st..key_ed].to_vec();

        Self {
            block,
            key: KeyVec::from_vec(key_raw.clone()),
            value_range,
            idx,
            first_key: KeyVec::from_vec(key_raw),
        }
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        let (st, ed) = self.value_range;
        &self.block.data[st..ed]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let (key_st, key_ed) = parse_range(&self.block.data[..]);
        let value_range = parse_range(&self.block.data[key_ed..]);

        let key = self.block.data[key_st..key_ed].to_vec();

        self.first_key = KeyVec::from_vec(key);
        self.key = self.first_key.clone();
        self.value_range = value_range;
        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        // If already reached the end, set empty key and return.
        if self.idx.eq(&self.block.offsets.len()) {
            self.key = KeyVec::new();
            return;
        }

        self.idx += 1;
        let offset = self.block.offsets[self.idx];
        let (key_st, key_ed) = parse_range(&self.block.data[offset as usize..]);
        let value_range = parse_range(&self.block.data[key_ed..]);

        self.key = KeyVec::from_vec(self.block.data[key_st..key_ed].to_vec());
        self.value_range = value_range;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let (idx, offset) = self.block.find_offset(key.raw_ref());

        let (key_st, key_ed) = parse_range(&self.block.data[offset as usize..]);
        let value_range = parse_range(&self.block.data[key_ed..]);

        let key_raw = self.block.data[key_st..key_ed].to_vec();

        self.key = KeyVec::from_vec(key_raw);
        self.value_range = value_range;
        self.idx = idx;
    }
}

/// Parse range of next item.
///
/// Please refer to the structure of `Entry` in `super::Block`.
fn parse_range(data: &[u8]) -> (usize, usize) {
    let len = u16::from_le_bytes(
        <[u8; 2]>::try_from(&data[0..2]).expect("unexpected error when parsing len"),
    ) as usize;
    (2, len + 2)
}
