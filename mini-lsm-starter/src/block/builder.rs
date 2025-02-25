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

use crate::key::{KeySlice, KeyVec};

use super::Block;

const KEY_PREFIX_LEN: usize = 4;
const KEY_VALUE_LEN: usize = 2 + KEY_PREFIX_LEN;

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

        let common_prefix_len = if self.is_empty() {
            self.first_key = KeyVec::from_vec(key_raw.clone());
            0
        } else {
            self.first_key
                .raw_ref()
                .iter()
                .zip(key_raw.iter())
                .take_while(|(a, b)| a == b)
                .count()
        };

        let new_entry_len = key_raw.len() - common_prefix_len + value.len() + KEY_VALUE_LEN;
        let current_block_len = self.data.len() + self.offsets.len() * 2;

        // Only the first key-value larger than block_size is allowed.
        if new_entry_len + current_block_len >= self.block_size && !self.is_empty() {
            return false;
        }

        // Convert to an Entry and push, record offset by the way.
        let entry = build_entry(common_prefix_len, &key_raw, value);
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
        let last_key = Block::init_last_key(&self.offsets, &self.data);
        Block {
            data: self.data,
            offsets: self.offsets,
            last_key,
        }
    }
}

fn build_entry(common_len: usize, key: &[u8], val: &[u8]) -> Vec<u8> {
    let mut key_part = build_key(common_len, key);
    let mut value_part = build_value(val);

    key_part.append(&mut value_part);
    key_part
}

fn build_key(common_len: usize, key: &[u8]) -> Vec<u8> {
    let overlapping_len = (common_len as u16).to_le_bytes();
    let rest_key_len = ((key.len() - common_len) as u16).to_le_bytes();
    let mut part = Vec::from(overlapping_len.as_ref());
    part.extend_from_slice(&rest_key_len);
    part.extend_from_slice(&key[common_len..]);
    part
}

fn build_value(data: &[u8]) -> Vec<u8> {
    let len = (data.len() as u16).to_le_bytes();
    let mut part = Vec::from(len.as_ref());
    part.extend_from_slice(data);
    part
}
