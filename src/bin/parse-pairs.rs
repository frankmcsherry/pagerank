use std::io::{Read, BufRead, BufReader, BufWriter, Write, Result};
use std::fs::File;
use std::slice;
use std::mem;

fn main() {

    let parse_mode = match std::env::args().skip(1).next().as_ref().map(|x| &x[..]) {
        Some("parse") => true,
        Some("print") => false,
        _ => panic!("parse_mode not one of print or parse"),
    };

    let stdin = std::io::stdin();
    let mut reader = stdin.lock();
    let mut writer = std::io::stdout();

    if parse_mode {

        let mut pairs = vec![0u32; 2];
        for readline in reader.lines() {
            let line = readline.ok().expect("read error");
            if !line.starts_with('#') {
                let elts: Vec<&str> = line[..].split("\t").collect();
                pairs[0] = elts[0].parse().ok().expect("malformed src");
                pairs[1] = elts[1].parse().ok().expect("malformed dst");
                writer.write_all(unsafe { typed_as_byte_slices(&pairs[..]) }).unwrap();
            }
        }

    }
    else {

        let mut bytes = vec![0u8; 1024];
        while let Ok(edges) = read_as_typed::<_,(u32,u32)>(&mut reader, &mut bytes) {
            if edges.len() == 0 { break; }
            for &(src,dst) in edges {
                println!("{} {}", src,dst);
            }
        }

    }
}

unsafe fn typed_as_byte_slices<T>(slice: &[T]) -> &[u8] {
    slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * mem::size_of::<T>())
}

// naughty method using unsafe transmute to read a filled binary buffer as a typed buffer
fn read_as_typed<'a, R: Read, T: Copy>(reader: &mut R, buffer: &'a mut[u8]) -> Result<&'a[T]> {
    if mem::size_of::<T>() * (buffer.len() / mem::size_of::<T>()) < buffer.len() {
        panic!("buffer size must be a multiple of mem::size_of::<T>() = {:?}", mem::size_of::<T>());
    }

    let mut read = try!(reader.read(buffer));
    while mem::size_of::<T>() * (read / mem::size_of::<T>()) < read {
        read += try!(reader.read(&mut buffer[read..]));
    }

    Ok(unsafe { ::std::slice::from_raw_parts_mut(buffer.as_mut_ptr() as *mut T, read / mem::size_of::<T>()) } )
}
