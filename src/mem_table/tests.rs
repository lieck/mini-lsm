use std::{ops::Bound, path::Path, sync::Arc};

use bytes::Bytes;

use super::MemTable;
use crate::{
    iterators::StorageIterator,
    sstable::{builder::SSTableBuilder, iterator::SSTableIterator},
};

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
fn test_memtable_get() {
    let mut memtable = MemTable::create();

    for i in 0..100 {
        let key = key_of(i);
        let val = value_of(i);
        memtable.put(&key, &val);
    }

    for i in 0..100 {
        let key = key_of(i);
        let val = memtable.get(&key).unwrap();
        assert_eq!(val, value_of(i));
    }

    for i in 0..50 {
        let key = key_of(i);
        let val = value_of(i + 100);
        memtable.put(&key, &val);
    }

    for i in 0..50 {
        let key = key_of(i);
        let val = memtable.get(&key).unwrap();
        assert_eq!(val, value_of(i + 100));
    }
}

#[test]
fn test_memtable_iter() {
    let mut memtable = MemTable::create();
    for i in 0..100 {
        let key = key_of(i);
        let val = value_of(i);
        memtable.put(&key, &val);
    }

    {
        let mut iter = memtable.scan(Bound::Unbounded, Bound::Unbounded);
        for i in 0..100 {
            assert!(iter.is_valid(), "{i}");
            let key = key_of(i);
            let value = value_of(i);
            assert_kv(i, &key, &value);
            iter.next().unwrap();
        }
    }

    for idx in 0..100 {
        let mut iter = memtable.scan(Bound::Included(&key_of(idx)), Bound::Unbounded);
        for i in idx..100 {
            assert!(iter.is_valid(), "{i}");
            let key = key_of(i);
            let value = value_of(i);
            assert_kv(i, &key, &value);
            iter.next().unwrap();
        }
    }

    {
        let mut iter = memtable.scan(Bound::Included(&key_of(12)), Bound::Excluded(&key_of(46)));
        for i in 12..46 {
            assert!(iter.is_valid(), "{i}");
            let key = key_of(i);
            let value = value_of(i);
            assert_kv(i, &key, &value);
            iter.next().unwrap();
        }
        assert!(!iter.is_valid());
    }
}

#[test]
fn test_memtable_to_sst() {
    let mut memtable = MemTable::create();
    for i in 0..100 {
        let key = key_of(i);
        let val = value_of(i);
        memtable.put(&key, &val);
    }

    let mut builder = SSTableBuilder::new(100);
    memtable.flush(&mut builder).unwrap();
    let sst = builder.build(1, None, Path::new("./tmp/test")).unwrap();
    let mut iter = SSTableIterator::create_and_seek_to_first(Arc::new(sst)).unwrap();
    for i in 0..100 {
        assert!(iter.is_valid(), "{i}");
        let key = key_of(i);
        let val = value_of(i);
        assert_kv(i, &key, &val);
        iter.next().unwrap();
    }
    assert!(!iter.is_valid());
}
