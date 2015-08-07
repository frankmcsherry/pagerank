use std::io::{BufRead, BufWriter, Write};
use std::fs::File;
use std::mem;

fn main() {
    println!("usage: parse-pairs <target>");
    println!("will overwrite <target>.pairs");
    let target = std::env::args().skip(1).next().unwrap();
    println!("target: {}", target);

    let mut pairs_writer = BufWriter::new(File::create(format!("{}.pairs", target)).unwrap());

    let input = std::io::stdin();
    for line in input.lock().lines().map(|x| x.unwrap()).filter(|x| !x.starts_with('#')) {
        let elts: Vec<&str> = line[..].split("\t").collect();
        let source: u32 = elts[0].parse().ok().expect("malformed source");
        let target: u32 = elts[1].parse().ok().expect("malformed target");
        pairs_writer.write(&unsafe { mem::transmute::<_, [u8; 4]>(source) }).unwrap();
        pairs_writer.write(&unsafe { mem::transmute::<_, [u8; 4]>(target) }).unwrap();
    }
}
