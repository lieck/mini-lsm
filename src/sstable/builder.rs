use std::{path::Path, sync::Arc};

use crate::{block::builder::BlockBuilder, lsm_storage::BlockCache};
use anyhow::{Ok, Result};
use bytes::BufMut;

use super::{BlockMeta, FileObject, SSTable};

///
#[derive(Debug)]
pub struct SSTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    max_block_size: usize,
    curr_block: BlockBuilder,
    data: Vec<u8>,
}

impl SSTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            meta: vec![],
            max_block_size: block_size,
            curr_block: BlockBuilder::new(0),
            data: vec![],
        }
    }

    /// Builds the SSTable and writes it to the given path.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SSTable> {
        // write the block
        let block = self.curr_block.build().encode();
        self.data.extend(block);

        let block_meta_offset = self.data.len() as u64;

        // wirte meta
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);

        // write meta offset
        self.data.put_u64(block_meta_offset);

        let file = FileObject::create(path.as_ref(), self.data)?;

        Ok(SSTable {
            sst_id : id,
            file,
            block_cache,
            block_meta_offset : block_meta_offset as usize,
            block_metas : self.meta
        })
    }

    /// Adds a key-value pair to SSTable
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if !self.curr_block.add(key, value) {
            let block =
                std::mem::replace(&mut self.curr_block, BlockBuilder::new(self.max_block_size));
            let block = block.build().encode();
            
            if block.len() > 2 {
                self.data.extend(block);
            }
        
            self.meta.push(BlockMeta {
                offset: self.data.len(),
                first_key: key.to_vec().into(),
            });
            
            if !self.curr_block.add(key, value) {
                panic!("key + val >= max_block_size");
            }
        }
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }
}
