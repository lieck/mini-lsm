
use bytes::Bytes;
use std::sync::Arc;

use super::{builder::BlockBuilder, Block};
use super::iterator::BlockIterator;

#[test]
fn test_block_build_single_key() {
    // key_len + val_len + num_of_elemnts = 6
    // key + val = 7
    // offset = 2
    {
        let mut builder = BlockBuilder::new(6 + 7 + 2);
        assert!(builder.add(b"123", b"4567"));
        assert!(!builder.add(b"", b""));
        _ = builder.build();
    }

    {
        let mut builder = BlockBuilder::new(6 + 7 + 1);
        assert!(!builder.add(b"123", b"4567"));
        _ = builder.build();
    }
}

fn key_of(val : usize) -> Vec<u8> {
    format!("key_{:03}", val).into_bytes()
}

fn value_of(val : usize) -> Vec<u8> {
    format!("val_{:010}", val).into_bytes()
}

fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

fn generate_block_size(idx : usize) -> Block {
    let mut builder = BlockBuilder::new(10000);
    for idx in 0..idx {
        let key = key_of(idx);
        let value = value_of(idx);
        assert!(builder.add(&key[..], &value[..]));
    }
    builder.build()
}

#[test]
fn test_block_build_add() {
    _ = generate_block_size(100);
}

#[test]
fn test_block_encode() {
    let block = generate_block_size(100);
    _ = block.encode();
}

#[test]
fn test_block_decode_empty() {
    let block = generate_block_size(0);
    let encoded = block.encode();
    let decoded_block = Block::decode(&encoded);
    assert_eq!(block.offsets, decoded_block.offsets);
    assert_eq!(block.data, decoded_block.data);
}

#[test]
fn test_block_decode_one() {
    let block = generate_block_size(1);
    let encoded = block.encode();
    let decoded_block = Block::decode(&encoded);
    assert_eq!(block.offsets, decoded_block.offsets);
    assert_eq!(block.data, decoded_block.data);
}

#[test]
fn test_block_decode() {
    let block = generate_block_size(100);
    let encoded = block.encode();
    let decoded_block = Block::decode(&encoded);
    assert_eq!(block.offsets, decoded_block.offsets);
    assert_eq!(block.data, decoded_block.data);
}



#[test]
fn test_block_multiple_keys() {
    let mut builder = BlockBuilder::new(300);
    for idx in 0..11 {
        let key = key_of(idx);
        let value = value_of(idx);
        assert!(builder.add(&key[..], &value[..]));
    }
    let block = builder.build();
    let block = Arc::new(block);
    let mut iter = BlockIterator::create_and_seek_to_first(block);
    for i in 0..11 {
        let key = iter.key();
        let value = iter.value();
        assert_eq!(
            key,
            key_of(i),
            "expected key: {:?}, actual key: {:?}",
            as_bytes(&key_of(i)),
            as_bytes(key)
        );
        assert_eq!(
            value,
            value_of(i),
            "expected value: {:?}, actual value: {:?}",
            as_bytes(&value_of(i)),
            as_bytes(value)
        );
        iter.next();
    }
}


#[test]
fn test_block_iterator() {
    let block = Arc::new(generate_block_size(100));
    let mut iter = BlockIterator::create_and_seek_to_first(block);
    for _ in 0..5 {
        for i in 0..100 {
            let key = iter.key();
            let value = iter.value();
            assert_eq!(
                key,
                key_of(i),
                "expected key: {:?}, actual key: {:?}",
                as_bytes(&key_of(i)),
                as_bytes(key)
            );
            assert_eq!(
                value,
                value_of(i),
                "expected value: {:?}, actual value: {:?}",
                as_bytes(&value_of(i)),
                as_bytes(value)
            );
            iter.next();
        }
        assert!(!iter.is_valid());
        iter.seek_to_first();
    }
}

#[test]
fn test_block_seek_key() {
    let block = Arc::new(generate_block_size(100));
    let mut iter = BlockIterator::create_and_seek_to_first(block);

    for _ in 0..5 {
        for start in 0..100 {
            let key = key_of(start);
            iter.seek_to_key(&key);

            for i in start..100 {
                let key = iter.key();
                let value = iter.value();
                assert_eq!(
                    key,
                    key_of(i),
                    "expected key: {:?}, actual key: {:?}",
                    as_bytes(&key_of(i)),
                    as_bytes(key)
                );
                assert_eq!(
                    value,
                    value_of(i),
                    "expected value: {:?}, actual value: {:?}",
                    as_bytes(&value_of(i)),
                    as_bytes(value)
                );
                iter.next();
            }
        }
    }
}
