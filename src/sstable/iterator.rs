use std::sync::Arc;

use crate::block::iterator::BlockIterator;
use crate::iterators::StorageIterator;

use super::SSTable;
use anyhow::{Ok, Result};

///
#[derive(Debug)]
pub struct SSTableIterator {
    table: Arc<SSTable>,
    block_iterator: BlockIterator,
    block_idx: usize,
}

impl SSTableIterator {
    /// Create a new iterator and seek to the first key-value pair.
    pub fn create_and_seek_to_first(table: Arc<SSTable>) -> Result<Self> {
        let read_block = table.read_block(0)?;
        let block_iterator = BlockIterator::create_and_seek_to_first(read_block);

        Ok(SSTableIterator {
            table,
            block_iterator,
            block_idx: 0,
        })
    }

    /// Seek to the first key-value pair.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let read_block = self.table.read_block(0)?;
        self.block_iterator = BlockIterator::create_and_seek_to_first(read_block);
        self.block_idx = 0;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SSTable>, key: &[u8]) -> Result<Self> {
        let block_idx = table.find_block_idx(key);
        let read_block = table.read_block(block_idx)?;
        let block_iterator = BlockIterator::create_and_seek_to_key(read_block, key);
        Result::Ok(SSTableIterator {
            table,
            block_iterator,
            block_idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        self.block_idx = self.table.find_block_idx(key);
        let read_block = self.table.read_block(self.block_idx)?;
        self.block_iterator = BlockIterator::create_and_seek_to_key(read_block, key);
        Result::Ok(())
    }
}

impl StorageIterator for SSTableIterator {
    fn key(&self) -> &[u8] {
        self.block_iterator.key()
    }

    fn value(&self) -> &[u8] {
        self.block_iterator.value()
    }

    fn is_valid(&self) -> bool {
        self.block_iterator.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.block_iterator.next();
        if !self.block_iterator.is_valid() {
            self.block_idx += 1;
            if self.block_idx >= self.table.block_metas.len() {
                return Ok(());
            }
            let block = self.table.read_block(self.block_idx)?;
            self.block_iterator = BlockIterator::create_and_seek_to_first(block);
        }
        Ok(())
    }
}
