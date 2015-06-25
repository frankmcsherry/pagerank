// use std::ptr;
use std::mem;

use timely::drain::DrainExt;
//
// pub fn qsort_kv<K: Ord, V>(keys: &mut [K], vals: &mut [V]) {
//     let mut work = vec![(keys, vals)];
//     while let Some((ks, vs)) = work.pop() {
//         if ks.len() < 16 { isort_kv(ks, vs); }
//         else {
//             let p = partition_kv(ks, vs);
//             let (ks1, ks2) = ks.split_at_mut(p);
//             let (vs1, vs2) = vs.split_at_mut(p);
//             work.push((&mut ks2[1..], &mut vs2[1..]));
//             work.push((ks1, vs1));
//         }
//     }
// }
//
// #[inline(always)]
// pub fn partition_kv<K: Ord, V>(keys: &mut [K], vals: &mut [V]) -> usize {
//
//     let pivot = keys.len() / 2;
//
//     let mut lower = 0;
//     let mut upper = keys.len() - 1;
//
//     unsafe {
//         while lower < upper {
//             // NOTE : Pairs are here to insulate against "same key" balance issues
//             while lower < upper && (keys.get_unchecked(lower),lower) <= (keys.get_unchecked(pivot),pivot) { lower += 1; }
//             while lower < upper && (keys.get_unchecked(pivot),pivot) <= (keys.get_unchecked(upper),upper) { upper -= 1; }
//             ptr::swap(keys.get_unchecked_mut(lower), keys.get_unchecked_mut(upper));
//             ptr::swap(vals.get_unchecked_mut(lower), vals.get_unchecked_mut(upper));
//         }
//     }
//
//     // we want to end up with xs[p] near lower.
//     if keys[lower] < keys[pivot] && lower < pivot { lower += 1; }
//     if keys[lower] > keys[pivot] && lower > pivot { lower -= 1; }
//     keys.swap(lower, pivot);
//     vals.swap(lower, pivot);
//     lower
// }
//
//
// // insertion sort
// pub fn isort_kv<K: Ord, V>(keys: &mut [K], vals: &mut [V]) {
//     for i in 1..keys.len() {
//         let mut j = i;
//         unsafe {
//             while j > 0 && keys.get_unchecked(j-1) > keys.get_unchecked(i) { j -= 1; }
//
//             // bulk shift the stuff we skipped over
//             let mut tmp_k: K = mem::uninitialized();
//             ptr::swap(&mut tmp_k, keys.get_unchecked_mut(i));
//             ptr::copy(keys.get_unchecked_mut(j), keys.get_unchecked_mut(j+1), i-j);
//             ptr::swap(&mut tmp_k, keys.get_unchecked_mut(j));
//             mem::forget(tmp_k);
//
//             let mut tmp_v: V = mem::uninitialized();
//             ptr::swap(&mut tmp_v, vals.get_unchecked_mut(i));
//             ptr::copy(vals.get_unchecked_mut(j), vals.get_unchecked_mut(j+1), i-j);
//             ptr::swap(&mut tmp_v, vals.get_unchecked_mut(j));
//             mem::forget(tmp_v);
//         }
//     }
// }


pub struct SegmentList<T> {
    size:     usize,
    segments: Vec<Vec<T>>,
    current:  Vec<T>,
}

impl<T> SegmentList<T> {
    pub fn push<I: Iterator<Item=T>>(&mut self, iterator: I) {
        for item in iterator {
            if self.current.len() == self.size {
                self.segments.push(mem::replace(&mut self.current, Vec::with_capacity(self.size)));
            }
            self.current.push(item);
        }
    }
    pub fn finalize(&mut self) -> Vec<Vec<T>> {
        if self.current.len() > 0 {
            self.segments.push(mem::replace(&mut self.current, Vec::with_capacity(self.size)));
        }
        mem::replace(&mut self.segments, Vec::new())
    }
    pub fn new(size: usize) -> SegmentList<T> {
        SegmentList {
            size:     size,
            segments: Vec::new(),
            current:  Vec::with_capacity(size),
        }
    }
}


pub fn radix_sort_32<V: Copy+Default, F: Fn(&V)->u32>(data: &mut Vec<Vec<V>>, free: &mut Vec<Vec<V>>, func: &F) {
    radix_shuf(data, free, &|x| ((func(x) >>  0) & 0xFF) as u8);
    radix_shuf(data, free, &|x| ((func(x) >>  8) & 0xFF) as u8);
    radix_shuf(data, free, &|x| ((func(x) >> 16) & 0xFF) as u8);
    radix_shuf(data, free, &|x| ((func(x) >> 24) & 0xFF) as u8);
}

pub fn radix_shuf<V: Copy+Default, F: Fn(&V)->u8>(data: &mut Vec<Vec<V>>, free: &mut Vec<Vec<V>>, func: &F) {

    let mut part = vec![]; for _ in 0..256 { part.push(free.pop().unwrap_or(Vec::with_capacity(1024))); }
    let mut full = vec![]; for _ in 0..256 { full.push(vec![]); }

    let buflen = 8;

    let mut temp = vec![Default::default(); buflen * 256];
    let mut counts = vec![0u8; 256];

    // loop through each buffer
    for mut vs in data.drain_temp() {
        for v in vs.drain_temp() {
            let key = func(&v) as usize;

            temp[buflen * key + counts[key] as usize] = v;
            counts[key] += 1;

            if counts[key] == buflen as u8 {
                for v in &temp[(buflen * key) .. ((buflen * key) + buflen)] { part[key].push(*v); }

                if part[key].len() == 1024 {
                    full[key].push(mem::replace(&mut part[key], free.pop().unwrap_or(Vec::new())));
                }

                counts[key] = 0;
            }
        }

        free.push(vs);
    }

    // check each partially filled buffer
    for (key, mut p) in part.drain_temp().enumerate() {
        for v in &temp[(buflen * key) .. ((buflen * key) + counts[key] as usize)] { p.push(*v); }

        if p.len() > 0 { full[key].push(p); }
        else           { free.push(p); }
    }

    // re-order buffers
    for mut cs in full.drain_temp() {
        for c in cs.drain_temp() { data.push(c); }
    }
}
