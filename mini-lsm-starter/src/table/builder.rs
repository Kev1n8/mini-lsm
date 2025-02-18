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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.extend_from_slice(key.raw_ref());
        }

        // Block could be full.
        if !self.builder.add(key, value) {
            let cur_builder =
                std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
            let blk = cur_builder.build();

            self.update_meta(blk);

            let retry = self.builder.add(key, value);
            debug_assert!(retry, "unexpected error when adding kv pair to block",);
        }
    }

    fn update_meta(&mut self, blk: super::Block) {
        let offset = self.data.len();
        let first_key = KeyBytes::from_bytes(Bytes::copy_from_slice(blk.first_key()));
        let last_key = KeyBytes::from_bytes(Bytes::copy_from_slice(blk.last_key()));

        self.data.extend_from_slice(&blk.encode());
        self.meta.push(BlockMeta {
            offset,
            first_key,
            last_key,
        });
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let mut data = self.data;
        let mut block_meta = self.meta;

        let first_key = KeyBytes::from_bytes(Bytes::copy_from_slice(self.first_key.as_ref()));

        let last_blk = self.builder.build();
        let last_key = KeyBytes::from_bytes(Bytes::copy_from_slice(last_blk.last_key()));
        // Update meta because the last block is built.
        {
            let offset = data.len();
            let first_key_blk = KeyBytes::from_bytes(Bytes::copy_from_slice(last_blk.first_key()));
            let last_key_blk = KeyBytes::from_bytes(Bytes::copy_from_slice(last_blk.last_key()));

            data.extend_from_slice(&last_blk.encode());
            block_meta.push(BlockMeta {
                offset,
                first_key: first_key_blk,
                last_key: last_key_blk,
            });
        }

        let block_meta_offset = data.len();
        BlockMeta::encode_block_meta(&block_meta, &mut data);

        let extra = (block_meta_offset as u32).to_le_bytes();
        data.extend_from_slice(extra.as_slice());

        let datasz = data.len();
        let file = FileObject::create(path.as_ref(), data)?;
        println!("SST built, info:");
        println!("    block_num: {}", block_meta.len());
        println!(
            "    first_key: {:?}, last_key: {:?}",
            Bytes::copy_from_slice(first_key.raw_ref()),
            Bytes::copy_from_slice(last_key.raw_ref()),
        );
        println!("    data size: {}", datasz);

        Ok(SsTable {
            file,
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    #[test]
    fn test_sst_encodec_single_block() {
        let mut builder = SsTableBuilder::new(100);
        builder.add(KeySlice::from_slice(b"key_001"), b"val_001");
        builder.add(KeySlice::from_slice(b"key_002"), b"val_002");

        let dir = tempdir().unwrap();
        let path = dir.path().join("basic_encode_decode.sst");
        let sst_expected = builder.build(0, None, path.clone()).unwrap();

        let file = FileObject::open(&path).unwrap();
        let sst = SsTable::open(0, None, file).unwrap();

        assert_eq!(sst.block_meta_offset, sst_expected.block_meta_offset);
        assert_eq!(sst.first_key, sst_expected.first_key);
        assert_eq!(sst.last_key, sst_expected.last_key);
        assert_eq!(sst.block_meta, sst_expected.block_meta);
    }

    #[test]
    fn test_sst_encodec_several_blocks() {
        let mut builder = SsTableBuilder::new(1);
        builder.add(KeySlice::from_slice(b"key_001"), b"val_001");
        builder.add(KeySlice::from_slice(b"key_002"), b"val_002");

        let dir = tempdir().unwrap();
        let path = dir.path().join("basic_encode_decode.sst");
        let sst_expected = builder.build(0, None, path.clone()).unwrap();

        let file = FileObject::open(&path).unwrap();
        let sst = SsTable::open(0, None, file).unwrap();

        assert_eq!(sst.block_meta.len(), 2);

        assert_eq!(sst.block_meta_offset, sst_expected.block_meta_offset);
        assert_eq!(sst.first_key, sst_expected.first_key);
        assert_eq!(sst.last_key, sst_expected.last_key);
        assert_eq!(sst.block_meta, sst_expected.block_meta);
    }
}
