extern crate mmap;
extern crate time;
extern crate timely;
extern crate getopts;

use timely::progress::timestamp::RootTimestamp;
use timely::dataflow::operators::*;
use timely::dataflow::channels::pact::Exchange;

mod typedrw;
mod graphmap;
mod sorting;
use graphmap::GraphMMap;
use sorting::{SegmentList, radix_sort_32};
use std::fs::File;
use std::io::{BufWriter, Write};

fn main () {

    let filename = std::env::args().skip(1).next().unwrap();
    let strategy = std::env::args().skip(2).next().unwrap() == "process";

    // currently need timely's full option set to parse args
    let mut opts = getopts::Options::new();
    opts.optopt("w", "workers", "", "");
    opts.optopt("p", "process", "", "");
    opts.optopt("n", "processes", "", "");
    opts.optopt("h", "hostfile", "", "");

    opts.optopt("o", "output", "", "");

    if let Ok(matches) = opts.parse(std::env::args().skip(3)) {

        let workers: usize = matches.opt_str("w").map(|x| x.parse().unwrap_or(1)).unwrap_or(1);

        let timely_opt_keys = ["w", "p", "n", "h"];
        let matches_copy = matches.clone();
        let timely_args: Vec<String> = timely_opt_keys.into_iter()
            .filter_map(|k| matches_copy.opt_str(k).map(|x| vec![format!("-{}", k), x]))
            .flat_map(|x| x)
            .collect();

        timely::execute_from_args(timely_args.into_iter(), move |root| {

            let index = root.index() as usize;
            let peers = root.peers() as usize;

            let start = time::precise_time_s();

            let nodes = GraphMMap::new(&filename).nodes();

            let mut segments = SegmentList::new(1024); // list of edge segments

            let mut src = vec![];   // holds ranks
            let mut deg = vec![];   // holds source degrees
            let mut rev = vec![];   // holds (dst, deg) pairs
            let mut trn = vec![];   // holds transposed sources

            let mut going = start;

            let peer_output_path = matches.opt_str("o").map(
                |p| replace_placeholder_or_append_path_suffix(&p, &index.to_string())
            );

            let mut input = root.dataflow(|builder| {

                let (input, edges) = builder.new_input::<(u32, u32)>();
                let (cycle, ranks) = builder.loop_variable::<(u32, f32)>(20, 1);

                let mut ranks = edges.binary_notify(&ranks,
                                    Exchange::new(|x: &(u32,u32)| x.0 as u64),
                                    Exchange::new(|x: &(u32,f32)| x.0 as u64),
                                    "pagerank",
                                    vec![RootTimestamp::new(0)],
                                    move |input1, input2, output, notificator| {

                    // receive incoming edges (should only be iter 0)
                    input1.for_each(|_iter, data| {
                        segments.push(data.drain(..));
                    });

                    // all inputs received for iter, commence multiplication
                    notificator.for_each(|iter,_,_| {

                        let now = time::now();

                        if index == 0 { println!("{}:{}:{}.{} starting iteration {}", now.tm_hour, now.tm_min, now.tm_sec, now.tm_nsec, iter.inner); }

                        // if the very first iteration, prepare some stuff.
                        // specifically, transpose edges and sort by destination.
                        if iter.inner == 0 {
                            let (a, b, c) = transpose(segments.finalize(), peers, nodes);
                            deg = a; rev = b; trn = c;
                            src = vec![0.0f32; deg.len()];
                        }

                        // record some timings in order to estimate per-iteration times
                        if iter.inner == 0  { println!("src: {}, dst: {}, edges: {}", src.len(), rev.len(), trn.len()); }
                        if iter.inner == 10 && index == 0 { going = time::precise_time_s(); }
                        if iter.inner == 20 && index == 0 { println!("average: {}", (time::precise_time_s() - going) / 10.0 ); }
                        if iter.inner == 20 && peer_output_path.is_some() {
                            match peer_output_path {
                                Some(ref path) => write_pagerank_values_to(&path, &src, index, peers, nodes),
                                None => {}
                            }
                        }

                        // prepare src for transmitting to destinations
                        for s in 0..src.len() { src[s] = (0.15 + 0.85 * src[s]) / deg[s] as f32; }

                        // wander through destinations
                        let mut trn_slice = &trn[..];
                        let mut session = output.session(&iter);
                        for &(dst, deg) in &rev {
                            let mut accum = 0.0;
                            for &s in &trn_slice[..deg as usize] {
                                // accum += src[s as usize];
                                unsafe { accum += *src.get_unchecked(s as usize); }
                            }
                            trn_slice = &trn_slice[deg as usize..];
                            session.give((dst, accum));
                        }

                        for s in &mut src { *s = 0.0; }
                    });

                    // receive data from workers, accumulate in src
                    input2.for_each(|iter, data| {
                        notificator.notify_at(iter);
                        for &(node, rank) in data.iter() {
                            src[node as usize / peers] += rank;
                        }
                    });
                });

                // optionally, do process-local accumulation
                if strategy {
                    let local_base = workers * (index / workers);
                    let local_index = index % workers;
                    let mut acc = vec![0.0; (nodes / workers) + 1];   // holds ranks
                    ranks = ranks.unary_notify(
                        Exchange::new(move |x: &(u32,f32)| (local_base as u64 + (x.0 as u64 % workers as u64))),
                        "aggregation",
                        vec![],
                        move |input, output, iterator| {
                            input.for_each(|iter, data| {
                                iterator.notify_at(iter);
                                for &(node, rank) in data.iter() {
                                    acc[node as usize / workers] += rank;
                                }
                            });

                            iterator.for_each(|item,_,_| {
                                output.session(&item)
                                      .give_iterator(acc.drain(..)
                                                        .enumerate()
                                                        .filter(|x| x.1 != 0.0)
                                                        .map(|(u,f)| ((u * workers + local_index) as u32, f)));

                                for _ in 0..(1 + (nodes/workers)) { acc.push(0.0); }
                            });
                        }
                    );
                }

                ranks.connect_loop(cycle);

                input
            });

            // introduce edges into the computation;
            // allow mmaped file to drop
            {
                let graph = GraphMMap::new(&filename);
                for node in 0..graph.nodes() {
                    if node % peers == index {
                        for dst in graph.edges(node) {
                            input.send(((node + 1) as u32, *dst as u32));
                        }
                    }
                }
            }
            input.close();
            while root.step() { }

            if index == 0 { println!("elapsed: {}", time::precise_time_s() - start); }
        }).unwrap();
    }
    else {
        println!("error parsing arguments");
        println!("usage:\tpagerank <source> (worker|process) [timely options]");
    }
}

// returns [src/peers] degrees, (dst, deg) pairs, and a list of [src/peers] endpoints
fn transpose(mut edges: Vec<Vec<(u32, u32)>>, peers: usize, nodes: usize) -> (Vec<u32>, Vec<(u32, u32)>, Vec<u32>)  {

    let mut deg = vec![0; (nodes as usize / peers) + 1];
    for list in &edges {
        for &(s, _) in list {
            deg[s as usize / peers] += 1;
        }
    }

    radix_sort_32(&mut edges, &mut Vec::new(), &|&(_,d)| d);

    let mut rev = Vec::<(u32,u32)>::with_capacity(deg.len());
    let mut trn = Vec::with_capacity(edges.len() * 1024);
    for list in edges {
        for (s,d) in list {
            let len = rev.len();
            if (len == 0) || (rev[len-1].0 < d) {
                rev.push((d, 0u32));
            }

            let len = rev.len();
            rev[len-1].1 += 1;
            trn.push(s / peers as u32);
        }
    }

    return (deg, rev, trn);
}

fn replace_placeholder_or_append_path_suffix(some_path: &str, value: &str) -> String {
    let result = str::replace(some_path, '?', value);
    if result != some_path {
        // its a different path (contained the placeholder), okay
        return result;
    }
    let p = std::path::Path::new(some_path);
    return format!("{:?}_{}{:?}", p.file_stem().unwrap(), value, p.extension().unwrap());
}

fn write_pagerank_values_to(output_path: &str, values: &Vec<f32>, peer: usize, peers: usize, nodes: usize) {
    println!("writing {} records to {}", values.len(), output_path);
    let mut pagerank_writer = BufWriter::new(File::create(output_path).unwrap());
    pagerank_writer.write("id\tpagerank\n".as_bytes()).unwrap();
    for i in 0..values.len() {
        let node_id = peer + i * peers;
        if node_id < nodes {
            pagerank_writer.write(format!("{}\t{}\n", node_id, values[i]).as_bytes()).unwrap();
        }
    }
}
