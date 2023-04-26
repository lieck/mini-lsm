use std::{fs, path::Path, sync::Arc};

use bytes::Bytes;
use rand::Rng;

use crate::sstable::{builder::SSTableBuilder, iterator::SSTableIterator};

use super::{merge_iterator::MergeIterator, StorageIterator};

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

fn generate_sstable_test<T, K>(size: usize, map: T, reduce: K)
where
    T: Fn(&mut Vec<SSTableBuilder>),
    K: Fn(Vec<Box<SSTableIterator>>),
{
    let mut builder = Vec::new();
    for _ in 0..size {
        builder.push(SSTableBuilder::new(300));
    }

    map(&mut builder);

    let mut rng = rand::thread_rng();
    let ran_num: u32 = rng.gen();

    let mut paths = Vec::new();
    for i in 0..size {
        let path = format!("./tmp/{}-{:05}", ran_num, i);
        paths.push(path);
    }

    let mut ssts = vec![];
    for (idx, sst) in builder.into_iter().enumerate() {
        ssts.push(Arc::new(
            sst.build(idx, None, Path::new(&paths[idx])).unwrap(),
        ));
    }

    let mut iters = vec![];
    for sst in ssts {
        iters.push(Box::new(
            SSTableIterator::create_and_seek_to_first(sst).unwrap(),
        ));
    }

    reduce(iters);

    for path in paths {
        fs::remove_file(path).unwrap();
    }
}

#[test]
fn test_merge_iterator_non_overlap() {
    let map = |sst: &mut Vec<SSTableBuilder>| {
        for i in 0..100 {
            let key = key_of(i);
            let value = value_of(i);
            if i & 1 == 0 {
                sst[0].add(&key, &value)
            } else {
                sst[1].add(&key, &value)
            }
        }
    };

    let reduce = |iters: Vec<Box<SSTableIterator>>| {
        let mut iter = MergeIterator::create(iters);
        for i in 0..100 {
            assert!(iter.is_valid(), "{i}");
            let key = iter.key();
            let value = iter.value();
            assert_kv(i, &key, &value);
            iter.next().unwrap();
        }
    };

    generate_sstable_test(2, map, reduce);
}

#[test]
fn test_merge_iterator_overlap() {
    let map = |sst: &mut Vec<SSTableBuilder>| {
        for idx in 0..3 {
            for i in 0..100 {
                let key = key_of(i);
                let value = value_of(0);
                sst[idx].add(&key, &value);
            }
        }
    };

    let reduce = |iters: Vec<Box<SSTableIterator>>| {
        let mut iter = MergeIterator::create(iters);
        for i in 0..100 {
            assert!(iter.is_valid(), "{i}");
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
                value_of(0),
                "expected value: {:?}, actual value: {:?}",
                as_bytes(&value_of(0)),
                as_bytes(value)
            );

            iter.next().unwrap();
        }
    };

    generate_sstable_test(3, map, reduce);
}

#[test]
fn test_merge_iterator_test1() {
    let map = |sst: &mut Vec<SSTableBuilder>| {
        for i in 0..100 {
            let key = key_of(i);
            if i & 1 == 0 {
                sst[0].add(&key, &value_of(0));
            } else {
                sst[1].add(&key, &value_of(1));
            }
            sst[2].add(&key, &value_of(2));
        }
    };

    let reduce = |iters: Vec<Box<SSTableIterator>>| {
        let mut iter = MergeIterator::create(iters);
        for i in 0..100 {
            assert!(iter.is_valid(), "{i}");
            let key = iter.key();
            let value = iter.value();

            assert_eq!(
                key,
                key_of(i),
                "expected key: {:?}, actual key: {:?}",
                as_bytes(&key_of(i)),
                as_bytes(key)
            );

            if i & 1 == 0 {
                assert_eq!(
                    value,
                    value_of(0),
                    "expected value: {:?}, actual value: {:?}",
                    as_bytes(&value_of(0)),
                    as_bytes(value)
                );
            } else {
                assert_eq!(
                    value,
                    value_of(1),
                    "expected value: {:?}, actual value: {:?}",
                    as_bytes(&value_of(1)),
                    as_bytes(value)
                );
            }

            iter.next().unwrap();
        }
    };

    generate_sstable_test(3, map, reduce);
}
