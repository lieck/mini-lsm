use std::sync::Arc;

use crate::block::Block;

/// A block
pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;
