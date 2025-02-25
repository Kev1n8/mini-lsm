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

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, Bytes};
pub use iterator::SsTableIterator;

use crate::block::{Block, BlockIterator};
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    ///
    /// BlockMeta layout:
    /// --------------------------------------------------------
    /// | offset | first_len | first_key | last_len | last_key |
    /// --------------------------------------------------------
    /// |  u32   |    u16    |   Bytes   |    u16   |   Bytes  |
    /// --------------------------------------------------------
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut v = Vec::new();
        for meta in block_meta {
            let offset = (meta.offset as u32).to_le_bytes();
            v.extend_from_slice(&offset);

            let first_len = (meta.first_key.len() as u16).to_le_bytes();
            v.extend_from_slice(&first_len);

            let first_key = meta.first_key.raw_ref();
            v.extend_from_slice(first_key);

            let last_len = (meta.last_key.len() as u16).to_le_bytes();
            v.extend_from_slice(&last_len);

            let last_key = meta.last_key.raw_ref();
            v.extend_from_slice(last_key);

            println!(
                "BlockMeta: offset={}, first_key={:?}, last_key={:?}",
                meta.offset, meta.first_key, meta.last_key
            );

            buf.append(&mut v);
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut buf = buf;
        let mut tmp = Vec::new();
        let mut result = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32_le();

            tmp.clear();
            let first_len = buf.get_u16_le();
            for _ in 0..first_len {
                tmp.push(buf.get_u8());
            }
            let first_key = KeyBytes::from_bytes(Bytes::copy_from_slice(&tmp));

            tmp.clear();
            let last_len = buf.get_u16_le();
            for _ in 0..last_len {
                tmp.push(buf.get_u8());
            }
            let last_key = KeyBytes::from_bytes(Bytes::copy_from_slice(&tmp));

            result.push(BlockMeta {
                offset: offset as usize,
                first_key,
                last_key,
            });
        }
        result
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
///
/// SSTable Layout:
/// --------------------------------------------------------------------------------------------------
/// |         Block Section         |                           Meta Section                         |
/// --------------------------------------------------------------------------------------------------
/// | data block | ... | data block | metadata | meta block offset (u32) | bloom data | bloom offset |
/// --------------------------------------------------------------------------------------------------
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let data = file.read(0, file.1)?;
        let len = data.len();

        // Parse Bloom filter.
        // Safety: Length is guarenteed.
        let mut st = len - 4;
        let bloom_filter_offset = u32::from_le_bytes(data[st..].try_into().unwrap()) as usize;

        let mut ed = st;
        st = bloom_filter_offset;
        let bloom_filter = Bloom::decode(&data[st..ed])?;

        // Parse block_offset.
        ed = bloom_filter_offset;
        st = ed - 4;
        let block_meta_offset = u32::from_le_bytes(data[st..ed].try_into().unwrap()) as usize;

        // Parse metadata.
        ed = st;
        st = block_meta_offset;
        let block_meta = BlockMeta::decode_block_meta(&data[st..ed]);

        // Parse first_key and last_key.
        let first_key = block_meta.first().unwrap().first_key.clone();
        let last_key = block_meta.last().unwrap().last_key.clone();

        Ok(Self {
            file,
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom_filter),
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_meta[block_idx].offset as u64;
        let len = if block_idx == self.block_meta.len() - 1 {
            self.block_meta_offset as u64 - offset
        } else {
            self.block_meta[block_idx + 1].offset as u64 - offset
        };
        let data = self.file.read(offset, len)?;
        Ok(Arc::new(Block::decode(data.as_slice())))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(cache) = &self.block_cache {
            let cached_key = (self.sst_id(), block_idx);
            if let Ok(block) = cache.try_get_with(cached_key, || self.read_block(block_idx)) {
                Ok(block)
            } else {
                Err(anyhow::Error::msg("error while getting block from cache"))
            }
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    ///
    /// Return `block.len()` if key not found (for sure).
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        // Binary search by first_key.
        match self
            .block_meta
            .binary_search_by_key(&key, |blk| blk.first_key.as_key_slice())
        {
            Ok(idx) => {
                // Simply found it. Rare situation.
                idx
            }
            Err(idx) => {
                // Normally, idx would be the next blk_idx of what we actually want.
                // Unless:
                // 1. The last key of block idx is smaller than key
                // 2. idx == blks_num
                if idx == 0 {
                    return idx;
                }

                if self.block_meta[idx - 1].last_key.as_key_slice() < key {
                    idx
                } else {
                    idx - 1
                }
            }
        }
    }

    /// Get block iterator of given idx and key.
    fn get_block_iter(&self, idx: usize, key: Option<KeySlice>) -> Result<BlockIterator> {
        let block = self.read_block_cached(idx)?;
        if let Some(key) = key {
            Ok(BlockIterator::create_and_seek_to_key(block, key))
        } else {
            Ok(BlockIterator::create_and_seek_to_first(block))
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    pub fn key_within(&self, key: KeySlice) -> bool {
        if let Some(bloom) = &self.bloom {
            if !bloom.may_contain(farmhash::fingerprint32(key.raw_ref())) {
                return false;
            }
        }
        self.first_key.as_key_slice() <= key && key <= self.last_key.as_key_slice()
    }

    pub fn range_overlaps(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
        let (l, r) = (self.first_key.raw_ref(), self.last_key.raw_ref());
        let lower_in = match lower {
            Bound::Included(lower) => l <= lower && r >= lower,
            Bound::Excluded(lower) => l < lower && r > lower,
            Bound::Unbounded => false,
        };
        let upper_in = match upper {
            Bound::Included(upper) => l <= upper && r >= upper,
            Bound::Excluded(upper) => l < upper && r > upper,
            Bound::Unbounded => false,
        };
        lower_in
            || upper_in
            || (matches!(lower, Bound::Unbounded) && matches!(upper, Bound::Unbounded))
    }
}
