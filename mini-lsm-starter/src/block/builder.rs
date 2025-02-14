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

use crate::key::{KeySlice, KeyVec};

use super::Block;

const KEY_VALUE_LEN: usize = 4;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let key_raw = Vec::from(key.raw_ref());
        let new_entry_len = key_raw.len() + value.len() + KEY_VALUE_LEN;
        let current_block_len = self.data.len() + self.offsets.len() * 2;

        // Only the first key-value larger than block_size is allowed.
        if new_entry_len + current_block_len >= self.block_size && !self.is_empty() {
            return false;
        }

        if self.is_empty() {
            self.first_key = KeyVec::from_vec(key_raw.clone());
        }

        // Convert to an Entry and push, record offset by the way.
        let entry = build_entry(&key_raw, value);
        let offset = self.data.len() as u16;
        for part in entry {
            self.data.push(part);
        }

        // Now update self.offset.
        self.offsets.push(offset);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}

fn build_entry(key: &[u8], val: &[u8]) -> Vec<u8> {
    let mut key_part = build_part(&key);
    let mut value_part = build_part(val);

    key_part.append(&mut value_part);
    key_part
}

fn build_part(data: &[u8]) -> Vec<u8> {
    let len = (data.len() as u16).to_le_bytes();
    let mut part = Vec::from(len.as_ref());
    part.extend_from_slice(data);
    part
}
