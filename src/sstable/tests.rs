use std::{fs, path::Path, sync::Arc};

use bytes::Bytes;

use crate::{block::Block, sstable::builder::SSTableBuilder};

use super::{iterator::SSTableIterator, SSTable};
use crate::iterators::StorageIterator;

fn sst_build_test<T, K>(id: usize, map: T, test: K)
where
    T: Fn(&mut SSTableBuilder),
    K: Fn(Arc<SSTable>),
{
    let path = format!("./tmp/test-{id}");
    let path = Path::new(&path);
    let mut builder = SSTableBuilder::new(300);

    map(&mut builder);

    let sst = builder.build(0, None, path).unwrap();
    let sst = Arc::new(sst);

    test(sst);

    fs::remove_file(path).unwrap();
}

fn key_of(val: usize) -> Vec<u8> {
    format!("key_{:05}", val).into_bytes()
}

fn value_of(val: usize) -> Vec<u8> {
    format!("val_{:010}", val).into_bytes()
}

fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

fn assert_kv(i: usize, key: &[u8], value: &[u8]) {
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
}

#[test]
fn test_sst_build_single_key() {
    let map = |builder: &mut SSTableBuilder| {
        builder.add(b"233", b"23333");
    };

    let test = |sst: Arc<SSTable>| {
        let it = SSTableIterator::create_and_seek_to_first(sst).unwrap();

        assert_eq!(it.key(), b"233");
        assert_eq!(it.value(), b"23333");
    };

    sst_build_test(1, map, test);
}

#[test]
fn test_sst_build_two_key() {
    let map = |builder: &mut SSTableBuilder| {
        builder.add(b"233", b"23333");
        builder.add(b"233", b"23333");
    };

    let test = |sst: Arc<SSTable>| {
        let mut it = SSTableIterator::create_and_seek_to_first(sst).unwrap();

        assert_eq!(it.key(), b"233");
        assert_eq!(it.value(), b"23333");
        it.next().unwrap();
        assert_eq!(it.key(), b"233");
        assert_eq!(it.value(), b"23333");
    };

    sst_build_test(2, map, test);
}

#[test]
fn test_sst_build_multiple_keys_print() {
    let map = |builder: &mut SSTableBuilder| {
        for i in 0..100 {
            let key = key_of(i);
            let value = value_of(i);
            builder.add(&key, &value);
        }
    };

    let test = |sst: Arc<SSTable>| {
        for i in 0..sst.block_metas.len() {
            let block = sst.read_block(i).unwrap();
            Block::dbeug_print(block);
        }
    };

    sst_build_test(3, map, test);
}

#[test]
fn test_sst_build_multiple_keys() {
    let map = |builder: &mut SSTableBuilder| {
        for i in 0..100 {
            let key = key_of(i);
            let value = value_of(i);
            builder.add(&key, &value);
        }
    };

    let test = |sst: Arc<SSTable>| {
        let mut iter = SSTableIterator::create_and_seek_to_first(sst).unwrap();
        for i in 0..100 {
            assert!(iter.is_valid(), "idx:{i}");

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

            iter.next().unwrap();
        }
    };

    sst_build_test(4, map, test);
}

#[test]
fn test_sst_build_multiple_keys_plus() {
    let map = |builder: &mut SSTableBuilder| {
        for i in 0..1090 {
            let key = key_of(i);
            let value = value_of(i);
            builder.add(&key, &value);
        }
    };

    let test = |sst: Arc<SSTable>| {
        let mut iter = SSTableIterator::create_and_seek_to_first(sst).unwrap();
        for i in 0..1000 {
            assert!(iter.is_valid(), "idx:{i}");

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

            iter.next().unwrap();
        }
    };

    sst_build_test(5, map, test);
}

#[test]
fn test_sst_iterator() {
    let map = |builder: &mut SSTableBuilder| {
        for i in 0..1100 {
            let key = key_of(i);
            let value = value_of(i);
            builder.add(&key, &value);
        }
    };

    let test = |sst: Arc<SSTable>| {
        for start in 0..1100 {
            let mut iter =
                SSTableIterator::create_and_seek_to_key(Arc::clone(&sst), &key_of(start)).unwrap();
            for i in start..1000 {
                assert!(iter.is_valid(), "idx:{i}");

                let key = iter.key();
                let value = iter.value();
                assert_kv(i, key, value);

                iter.next().unwrap();
            }
        }
    };

    sst_build_test(6, map, test);
}
