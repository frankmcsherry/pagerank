use std::io::{BufRead, BufReader, BufWriter, Write};
use std::fs::File;
use std::slice;
use std::mem;

fn main() {
    println!("usage: parse <source> <target>");
    println!("will overwrite <target>.offsets and <target>.targets");
    let source = std::env::args().skip(1).next().unwrap();
    let target = std::env::args().skip(2).next().unwrap();

    let graph = read_edges(&source);
    digest_graph_vector(&_extract_fragment(graph.iter().map(|x| *x)), &target);
}

// loads the read_edges file available at https://snap.stanford.edu/data/soc-LiveJournal1.html
fn read_edges(filename: &str) -> Vec<(u32, u32)> {
    let mut graph = Vec::new();
    let file = BufReader::new(File::open(filename).unwrap());
    for readline in file.lines() {
        let line = readline.ok().expect("read error");
        if !line.starts_with('#') {
            let elts: Vec<&str> = line[..].split("\t").collect();
            let src: u32 = elts[0].parse().ok().expect("malformed src");
            let dst: u32 = elts[1].parse().ok().expect("malformed dst");
            graph.push((src, dst))
        }
    }

    println!("graph data loaded; {:?} edges", graph.len());
    return graph;
}

fn _extract_fragment<I: Iterator<Item=(u32, u32)>>(graph: I) -> (Vec<u64>, Vec<u32>) {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for (src, dst) in graph {
        while src + 1 >= nodes.len() as u32 { nodes.push(0); }
        while dst + 1 >= nodes.len() as u32 { nodes.push(0); } // allows unsafe access to nodes

        nodes[src as usize + 1] += 1;
        edges.push(dst);
    }

    for index in (1..nodes.len()) {
        nodes[index] += nodes[index - 1];
    }

    return (nodes, edges);
}

fn digest_graph_vector(graph: &(Vec<u64>, Vec<u32>), output_prefix: &str) {
    let mut edge_writer = BufWriter::new(File::create(format!("{}.targets", output_prefix)).unwrap());
    let mut node_writer = BufWriter::new(File::create(format!("{}.offsets", output_prefix)).unwrap());
    node_writer.write_all(unsafe { _typed_as_byte_slice(&graph.0[..]) }).unwrap();

    let mut slice = unsafe { _typed_as_byte_slice(&graph.1[..]) };
    while slice.len() > 0 {
        let to_write = if slice.len() < 1000000 { slice.len() } else { 1000000 };
        edge_writer.write_all(&slice[..to_write]).unwrap();
        println!("wrote some; remaining: {}", slice.len());
        slice = &slice[to_write..];
    }
}

unsafe fn _typed_as_byte_slice<T>(slice: &[T]) -> &[u8] {
    slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * mem::size_of::<T>())
}
