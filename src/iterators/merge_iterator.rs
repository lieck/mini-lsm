use anyhow::Ok;

use super::StorageIterator;
use std::{cmp, collections::BinaryHeap};

/// HeapWrapper
#[derive(Debug)]
struct HeapWrapper<I: StorageIterator>(usize, Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(other.1.key()) {
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
        }.map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, perfer the one with smaller index.
#[derive(Debug)]
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    ///
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        let mut heap = BinaryHeap::new();

        if iters.iter().all(|x| !x.is_valid()) {
            let mut iters = iters;
            return Self {
                iters : heap,
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            }
        }

        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        } 
        
        let current = heap.pop();
        Self {
            iters : heap,
            current
        }
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn key(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        let mut current = std::mem::replace(&mut self.current, None).unwrap();

        while !self.iters.is_empty() {
            if self.iters.peek_mut().unwrap().1.key() == current.1.key() {
                let mut iter = self.iters.pop().unwrap();
                iter.1.next().unwrap();
                if iter.1.is_valid() {
                    self.iters.push(iter);
                }
            } else {
                break;
            }
        }

        current.1.next().unwrap();
        if current.1.is_valid() {
            self.iters.push(current);
        }

        self.current = self.iters.pop();
    
        Ok(())
    }
}
