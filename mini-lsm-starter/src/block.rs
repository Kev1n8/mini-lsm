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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

const ERR_MSG: &str = "block touched outside memory unexpectedly";

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
///
/// TODO(Kaifeng): Split Block into DataBlock and OffsetBlock.
#[derive(Debug, PartialEq, Eq)]
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
    last_key: Vec<u8>,
}

impl Block {
    /// Block:
    ///
    /// ----------------------------------------------------------------------------------------------------
    /// |             Data Section             |              Offset Section             |      Extra      |
    /// ----------------------------------------------------------------------------------------------------
    /// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
    /// ----------------------------------------------------------------------------------------------------
    ///
    /// Entry(with out prefix):
    ///
    /// -----------------------------------------------------------------------
    /// |                           Entry #1                            | ... |
    /// -----------------------------------------------------------------------
    /// | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
    /// -----------------------------------------------------------------------
    ///
    /// Offset + Extra:
    /// -------------------------------
    /// |offset|offset|num_of_elements|
    /// -------------------------------
    /// |   0  |  12  |       2       |
    /// -------------------------------
    ///
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut encoded = Vec::with_capacity(self.data.len() + self.offsets.len() * 2 + 2);

        encoded.extend_from_slice(&self.data);

        for offset in &self.offsets {
            encoded.extend_from_slice(&offset.to_le_bytes());
        }

        encoded.extend_from_slice(&(self.offsets.len() as u16).to_le_bytes());

        Bytes::from(encoded)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let raw = data;
        let len = raw.len();

        // Last two u8 is num_of_elements in le bytes.
        let slice: [u8; 2] = raw[len - 2..len].try_into().expect(ERR_MSG);
        let num_of_elements = u16::from_le_bytes(slice) as usize;

        // Decode offsets.
        let mut offsets = Vec::<u16>::with_capacity(num_of_elements);
        let (off_st, off_ed) = (len - 2 - num_of_elements * 2, len - 2); // [st, ed) of offset section.
        for off in (off_st..off_ed).step_by(2) {
            let slice: [u8; 2] = raw[off..off + 2].try_into().expect(ERR_MSG);
            offsets.push(u16::from_le_bytes(slice));
        }

        // Decode data.
        let mut data = Vec::new();
        for (st, ed) in offsets.iter().zip(offsets.iter().skip(1)) {
            let (st, ed) = (*st as usize, *ed as usize);
            let v = Vec::from(&raw[st..ed]);
            data.push(v);
        }
        // Remember to handle the last one.
        let (last_st, last_ed) = (*offsets.last().expect(ERR_MSG) as usize, off_st);
        data.push(Vec::from(&raw[last_st..last_ed]));

        let data = data.into_iter().flatten().collect::<Vec<_>>();

        let last_key = Block::init_last_key(&offsets, raw);

        Self {
            data,
            offsets,
            last_key,
        }
    }

    /// Return the offset of given start key.
    pub fn find_offset(&self, key: &[u8]) -> (usize, u16) {
        if self.offsets.is_empty() {
            return (0, 0);
        }
        // TODO: Use binary search.
        let first_key = self.first_key();
        for (i, &offset) in self.offsets.iter().enumerate() {
            let (rest_st, rest_ed, common_len) = parse_key_range(&self.data, offset as usize);
            let cur_key = Vec::from_iter(
                first_key[..common_len]
                    .iter()
                    .cloned()
                    .chain(self.data[rest_st..rest_ed].iter().cloned()),
            );

            if cur_key.as_slice() >= key {
                return (i, offset);
            }
        }
        (self.offsets.len() - 1, *self.offsets.last().expect(ERR_MSG))
    }

    /// Return the first key of the block.
    pub fn first_key(&self) -> &[u8] {
        if self.offsets.is_empty() {
            panic!("calling `first_key` on an empty block")
        }
        let (st, ed, _common_len_is_zero) = parse_key_range(&self.data, 0);
        &self.data[st..ed]
    }

    /// Return the last key of the block.
    pub fn last_key(&self) -> &[u8] {
        if self.offsets.is_empty() {
            panic!("calling `last_key` on an empty block")
        }
        &self.last_key
    }

    pub fn init_last_key(offsets: &[u16], data: &[u8]) -> Vec<u8> {
        let offset = offsets.last().unwrap();
        let (rest_st, rest_ed, common_len) = parse_key_range(data, *offset as usize);
        let rest = &data[rest_st..rest_ed];

        // Could be the first key.(only contains 1 key in this block)
        if common_len.eq(&0) {
            rest.to_vec()
        } else {
            let (st, _ed, _common_len_is_zero) = parse_key_range(data, 0);
            let prefix = &data[st..st + common_len];
            let mut last_key = Vec::with_capacity(prefix.len() + rest.len());
            last_key.extend_from_slice(prefix);
            last_key.extend_from_slice(rest);
            last_key
        }
    }
}

/// Parse range of next key.
///
/// Return (rest_key_st, rest_key_ed, common_prefix_len).
fn parse_key_range(data: &[u8], offset: usize) -> (usize, usize, usize) {
    let overlapping_len = u16::from_le_bytes(
        <[u8; 2]>::try_from(&data[offset..offset + 2]).expect("unexpected error when parsing len"),
    ) as usize;
    let rest_len = u16::from_le_bytes(
        <[u8; 2]>::try_from(&data[offset + 2..offset + 4])
            .expect("unexpected error when parsing len"),
    ) as usize;
    (offset + 4, offset + rest_len + 4, overlapping_len)
}

/// Parse range of next value.
fn parse_value_range(data: &[u8], offset: usize) -> (usize, usize) {
    let len = u16::from_le_bytes(
        <[u8; 2]>::try_from(&data[offset..offset + 2]).expect("unexpected error when parsing len"),
    ) as usize;
    (offset + 2, offset + len + 2)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn concat_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
        let mut value = Vec::with_capacity(a.len() + b.len());
        value.extend_from_slice(a);
        value.extend_from_slice(b);
        value
    }

    #[test]
    fn basic_encoding_decoding() {
        let value = [4u8, 5, 6];
        let len = 3u16.to_le_bytes(); // len of key and value.

        let key = [1u8, 2, 3];
        let overlapping_len = 0u16.to_le_bytes();
        let rest_key_len = 3u16.to_le_bytes();

        let key = Vec::from(key);
        let value = Vec::from(value);

        let key_part = concat_bytes(&concat_bytes(&overlapping_len, &rest_key_len), key.as_ref());
        let value_part = concat_bytes(len.as_ref(), value.as_ref());
        let entry = concat_bytes(&key_part, &value_part);
        let encoded = concat_bytes(&entry, 0u16.to_le_bytes().as_ref());
        let expected = concat_bytes(&encoded, 1u16.to_le_bytes().as_ref());

        let block = Block {
            data: entry,
            offsets: Vec::from([0u16]),
            last_key: key.clone(),
        };

        assert_eq!(block.encode(), Bytes::from(expected.clone()));

        let block_decoded = Block::decode(&expected);
        assert_eq!(block_decoded, block);
        assert_eq!(block_decoded.last_key, key.clone());
    }
}
