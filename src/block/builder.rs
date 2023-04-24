use super::Block;

/// 
#[derive(Debug)]
pub struct BlockBuilder {
    data : Vec<u8>,
    offsets : Vec<u16>,
    block_size : usize,
    curr_size : usize,
}

impl BlockBuilder {
    /// 
    pub fn new(block_size: usize) -> Self {
        Self {
            data: vec![],
            offsets: vec![],
            block_size,
            curr_size : 2,
        }
    }

    ///
    pub fn add(&mut self, key : &[u8], value: &[u8]) -> bool {
        let key_len = key.len();
        let value_len = value.len();
        let add_len = 6 + key_len + value_len;

        if self.curr_size + add_len > self.block_size {
            return false;
        }

        let curr_offset = self.data.len() as u16;
        self.offsets.push(curr_offset);

        self.data.extend_from_slice(&(key_len as u16).to_be_bytes());
        self.data.extend_from_slice(key);

        self.data.extend_from_slice(&(value_len as u16).to_be_bytes());
        self.data.extend_from_slice(value);
        
        self.curr_size += 6 + key_len + value_len;
        true
    }

    ///
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}