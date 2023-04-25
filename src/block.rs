use bytes::BufMut;
use bytes::{Buf, Bytes};


///
pub mod builder;

/// Create
pub mod iterator;

/// block
#[derive(Debug)]
pub struct Block {
    /// data
    data: Vec<u8>,

    /// offset
    offsets: Vec<u16>,
}

impl Block {
    ///
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        let offsets_len = self.offsets.len();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.put_u16(offsets_len as u16);
        buf.into()
    }

    ///
    pub fn decode(data: &[u8]) -> Self {
        let mut idx = data.len() - 2;
        let num_of_elemnts = (&data[idx..]).get_u16() as usize;

        let mut offsets = Vec::new();
        offsets.reserve(num_of_elemnts);

        idx -= num_of_elemnts << 1;
        for _ in 0..num_of_elemnts {
            offsets.push((&data[idx..idx + 2]).get_u16());
            idx += 2;
        }

        if num_of_elemnts == 0 {
            return Block {
                data: Vec::new(),
                offsets,
            };
        }

        let mut data_len = offsets[offsets.len() - 1] as usize;
        // data_len += [ket_len] + [key]
        data_len += (&data[data_len..data_len + 2]).get_u16() as usize + 2;
        // data_len += [ket_len] + [key]
        data_len += (&data[data_len..data_len + 2]).get_u16() as usize + 2;

        Block {
            data: data[0..data_len].to_vec(),
            offsets,
        }
    }


    ///
    #[cfg(test)]
    pub fn dbeug_print(block : std::sync::Arc<Block>) {
        use self::iterator::BlockIterator;

        let mut iter = BlockIterator::create_and_seek_to_first(block);
        while iter.is_valid() {
            let key = Bytes::copy_from_slice(iter.key());
            let value = Bytes::copy_from_slice(iter.value());
            log::debug!("key:{:?}, value:{:?}", key, value);
            iter.next();
        }
    }
}

#[cfg(test)]
mod tests;