use std::io::{BufRead, BufReader, BufWriter, Write};
use std::fs::File;
use std::slice;
use std::mem;

fn main() {

    let stdin = std::io::stdin();
    let reader = stdin.lock();
    let mut writer = std::io::stdout();

    let mut pairs = vec![0u32; 2];
    for readline in reader.lines() {
        let line = readline.ok().expect("read error");
        if !line.starts_with('#') {
            let elts: Vec<&str> = line[..].split("\t").collect();
            pairs[0] = elts[0].parse().ok().expect("malformed src");
            pairs[1] = elts[1].parse().ok().expect("malformed dst");
            writer.write_all(unsafe { _typed_as_byte_slice(&pairs[..]) }).unwrap();
        }
    }
}

unsafe fn _typed_as_byte_slice<T>(slice: &[T]) -> &[u8] {
    slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * mem::size_of::<T>())
}
