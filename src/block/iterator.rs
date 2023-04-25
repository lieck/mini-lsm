

use std::sync::Arc;
use bytes::Buf;

use super::Block;


/// Block Iterator
#[derive(Debug)]
pub struct BlockIterator {
    /// block
    block : Arc<Block>,

    /// key
    key : Vec<u8>,

    /// value
    value : Vec<u8>,

    /// idx
    idx : usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut it = Self::new(block);
        it.set_entry_idx(0);
        it
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut it = Self::create_and_seek_to_first(block);
        it.seek_to_key(key);
        it
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        self.idx < self.block.offsets.len()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        if self.block.offsets.len() >= 1 {
            self.set_entry_idx(0);
        }
    }
    
    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let mut l = 0;
        let mut r = self.block.offsets.len() - 1;
        while l < r {
            let m = (l + r) >> 1;

            let mut office = self.block.offsets[m] as usize;
            let key_len = (&self.block.data[office..office + 2]).get_u16() as usize;
            office += 2;

            // arr[m] < key
            if &self.block.data[office..office + key_len] < key {
                l = m + 1;
            } else {
                r = m;
            }
        }

        self.set_entry_idx(l);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        if self.idx < self.block.offsets.len() {
            self.set_entry_idx(self.idx);
        }
    }

    fn set_entry_idx(&mut self, idx : usize) {
        let mut offset = self.block.offsets[idx] as usize;

        let len = (&self.block.data[offset..offset + 2]).get_u16() as usize;
        offset += 2;
        self.key = self.block.data[offset..offset + len].to_vec();

        offset += len;
        
        let len = (&self.block.data[offset..offset + 2]).get_u16() as usize;
        offset += 2;
        self.value = self.block.data[offset..offset + len].to_vec();

        self.idx = idx;
    }

}