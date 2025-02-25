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

use anyhow::{Ok, Result};

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        debug_assert!(!table.block_meta.is_empty(), "sst has no block meta");

        let iter = table.get_block_iter(0, None)?;

        Ok(Self {
            table,
            blk_iter: iter,
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        // Iterator could be right on the first block.
        if self.blk_idx.eq(&0) {
            self.blk_iter.seek_to_first();
            return Ok(());
        }
        debug_assert!(!self.table.block_meta.is_empty(), "sst has no block meta");

        let _ = std::mem::replace(&mut self.blk_iter, self.table.get_block_iter(0, None)?);

        self.blk_idx = 0;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    ///
    /// The iter could be born invalid.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let block_num = table.block_meta.len();
        let blk_idx = table.find_block_idx(key);

        // If idx == block.len(), it means key is beyond all blocks.
        let iter = if blk_idx.ge(&block_num) {
            // The iter will be born invalid.
            Self {
                table,
                blk_iter: BlockIterator::create_dummy(),
                blk_idx: block_num,
            }
        } else {
            let blk_iter = table.get_block_iter(blk_idx, Some(key))?;

            Self {
                table,
                blk_iter,
                blk_idx,
            }
        };

        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let block_num = self.table.block_meta.len();
        let idx = self.table.find_block_idx(key);

        // If idx is invalid.
        if idx.ge(&block_num) {
            self.blk_iter = BlockIterator::create_dummy();
            self.blk_idx = block_num;
            return Ok(());
        }

        // Could be right at this block.
        if self.blk_idx.eq(&idx) {
            self.blk_iter.seek_to_key(key);
            return Ok(());
        }

        // Otherwise, we have to read the block and create block.
        let _ = std::mem::replace(
            &mut self.blk_iter,
            self.table.get_block_iter(idx, Some(key))?,
        );
        self.blk_idx = idx;

        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    ///
    /// If `idx` has already reached `block_meta.len()`, it's invalid.
    fn is_valid(&self) -> bool {
        self.blk_idx.lt(&self.table.block_meta.len())
    }

    /// Move to the next `key` in the block.
    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Err(anyhow::Error::msg("calling next on an invalid sst iter"));
        }

        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;

            // Could be invalid.
            if !self.is_valid() {
                self.blk_iter = BlockIterator::create_dummy();
                return Ok(());
            }

            let _ = std::mem::replace(
                &mut self.blk_iter,
                self.table.get_block_iter(self.blk_idx, None)?,
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::table::SsTableBuilder;

    use super::*;

    #[test]
    fn test_sst_iter_single_block() {
        let mut builder = SsTableBuilder::new(100);
        builder.add(KeySlice::from_slice(b"key_001"), b"val_001");
        builder.add(KeySlice::from_slice(b"key_002"), b"val_002");

        let dir = tempdir().unwrap();
        let path = dir.path().join("basic_encode_decode.sst");
        let sst = builder.build(0, None, path.clone()).unwrap();

        let mut iter = SsTableIterator::create_and_seek_to_first(Arc::new(sst)).unwrap();

        assert_eq!(iter.key(), KeySlice::from_slice(b"key_001"));
        iter.next().unwrap();
        assert_eq!(iter.key(), KeySlice::from_slice(b"key_002"));
        iter.next().unwrap();
        assert!(!iter.is_valid());
    }

    #[test]
    fn test_sst_iter_several_blocks() {
        let mut builder = SsTableBuilder::new(1);
        builder.add(KeySlice::from_slice(b"key_001"), b"val_001");
        builder.add(KeySlice::from_slice(b"key_002"), b"val_002");

        let dir = tempdir().unwrap();
        let path = dir.path().join("basic_encode_decode.sst");
        let sst = builder.build(0, None, path.clone()).unwrap();

        assert_eq!(sst.block_meta.len(), 2);

        let mut iter = SsTableIterator::create_and_seek_to_first(Arc::new(sst)).unwrap();

        assert_eq!(iter.key(), KeySlice::from_slice(b"key_001"));
        iter.next().unwrap();
        assert_eq!(iter.key(), KeySlice::from_slice(b"key_002"));
        iter.next().unwrap();
        assert!(!iter.is_valid());
    }
}
