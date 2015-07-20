use std::io::{BufRead, BufReader, BufWriter, Write};
use std::fs::File;
use std::slice;
use std::mem;

fn main() {
    println!("usage: parse-pairs <source> <target>");
    println!("will overwrite <target>.pairs");
    let source = std::env::args().skip(1).next().unwrap();
    let target = std::env::args().skip(2).next().unwrap();

    let file = BufReader::new(File::open(source).unwrap());
    let mut writer = BufWriter::new(File::create(format!("{}.pairs", target)).unwrap());

    let mut pairs = vec![0u32; 2];
    for readline in file.lines() {
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
