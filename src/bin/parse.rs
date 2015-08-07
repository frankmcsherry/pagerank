use std::io::{BufRead, BufWriter, Write};
use std::fs::File;
use std::mem;

// output file format is
//
//     offset: [u64; max_src_node_id+1],
//     target: [u32; edges],
//
// target[offset[i]..offset[i+1]] are node i's edge targets.

fn main() {
    println!("usage: parse <target>");
    println!("will overwrite <target>.offsets and <target>.targets");
    let target = std::env::args().skip(1).next().unwrap();
    println!("target: {}", target);

    let mut node_writer = BufWriter::new(File::create(format!("{}.offsets", target)).unwrap());
    let mut edge_writer = BufWriter::new(File::create(format!("{}.targets", target)).unwrap());

    let mut cur_source = 0u32;
    let mut cur_offset = 0u64;
    let mut max_vertex = 0u32;

    let input = std::io::stdin();
    for line in input.lock().lines().map(|x| x.unwrap()).filter(|x| !x.starts_with('#')) {

        let elts: Vec<&str> = line[..].split("\t").collect();
        let source: u32 = elts[0].parse().ok().expect("malformed source");
        let target: u32 = elts[1].parse().ok().expect("malformed target");

        while cur_source < source {
            node_writer.write(&unsafe { mem::transmute::<_, [u8; 8]>(cur_offset) }).unwrap();
            cur_source += 1;
        }

        max_vertex = std::cmp::max(max_vertex, source);
        max_vertex = std::cmp::max(max_vertex, target);

        edge_writer.write(&unsafe { mem::transmute::<_, [u8; 4]>(target) }).unwrap();
        cur_offset += 1;
    }
    
    // a bit of a waste. convenient.
    while cur_source <= max_vertex {
        node_writer.write(&unsafe { mem::transmute::<_, [u8; 8]>(cur_offset) }).unwrap();
        cur_source += 1;
    }
}
