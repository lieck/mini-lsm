use anyhow::{Ok, Result};
use bytes::{Buf, BufMut, Bytes};
use std::{
    fmt::Debug,
    fs::{File, OpenOptions},
    io::Write,
    os::unix::prelude::FileExt,
    path::Path,
    sync::Arc,
};

use crate::{block::Block, lsm_storage::BlockCache};

///
pub mod builder;

///
pub mod iterator;

/// blcok meta
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut buf_len = block_meta.len() * 6;
        for meta in block_meta {
            buf_len += meta.first_key.len();
        }

        buf.reserve(buf_len);

        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(&meta.first_key);
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;

            let first_key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);

            block_meta.push(BlockMeta { offset, first_key });
        }
        block_meta
    }
}

/// A file object.
#[derive(Debug)]
pub struct FileObject(File, u64);

impl FileObject {
    /// Create a new file object and write the file to the disk.
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        assert_ne!(data.len(), 0);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        file.write_all(&data)?;
        Ok(FileObject(file, data.len() as u64))
    }

    /// open a file
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::create(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(file, size))
    }

    /// read a file
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let mut buf = vec![0; len as usize];
        self.0.read_exact_at(&mut buf, offset).unwrap();
        Ok(buf)
    }
}

/// sstable
#[derive(Debug)]
pub struct SSTable {
    sst_id: usize,
    file: FileObject,
    block_metas: Vec<BlockMeta>,
    block_meta_offset: usize,
    block_cache: Option<Arc<BlockCache>>,
}

impl SSTable {
    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let file_len = file.1;

        let block_meta_offset = file.read(0, file_len)?;
        let block_meta_offset = (&block_meta_offset[0..]).get_u64();

        let block_meta_len = file_len - 8 - block_meta_offset;

        let metas_data = file.read(block_meta_offset, block_meta_len)?;
        let block_metas = BlockMeta::decode_block_meta(&metas_data[0..]);

        Ok(Self {
            sst_id: id,
            file,
            block_metas,
            block_meta_offset: block_meta_offset as usize,
            block_cache,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_metas[block_idx].offset as u64;
        let offset_end = self
            .block_metas
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset) as u64;

        let data = self.file.read(offset, offset_end - offset)?;
        Ok(Arc::new(Block::decode(&data)))
    }

    /// Read a block from disk, with block cache.
    pub fn read_block_cached(&mut self, block_idx: usize) -> Result<Arc<Block>> {
        if self.block_cache.is_none() {
            return Err(anyhow::anyhow!("block cache is not set"));
        }

        let cache = self.block_cache.as_mut().unwrap();
        let block = cache.get(&(self.sst_id, block_idx)).unwrap_or_else(|| {
            let block = self.read_block(block_idx).unwrap();
            self.block_cache
                .as_ref()
                .unwrap()
                .insert((self.sst_id, block_idx), Arc::clone(&block));
            block
        });
        Ok(block)
    }

    /// Find the block that may contain `key`.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        let mut l = 0;
        let mut r = self.block_metas.len() - 1;
        while l < r {
            let m = (l + r + 1) >> 1;
            if key >= self.block_metas[m].first_key {
                l = m;
            } else {
                r = m - 1;
            }
        }
        l
    }
}

///
#[cfg(test)]
mod tests;
