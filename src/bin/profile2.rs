
extern crate rand;
extern crate time;
extern crate timely;
extern crate timely_sort;

use rand::{Rng, SeedableRng, StdRng};

use timely::progress::timestamp::RootTimestamp;
use timely::dataflow::operators::{Input, Operator, LoopVariable, ConnectLoop};
use timely::dataflow::channels::pact::Exchange;

use timely_sort::{RadixSorter, RadixSorterBase};
use timely_sort::LSBRadixSorter as Sorter;

fn main () {

    let node_cnt = std::env::args().skip(1).next().unwrap().parse::<usize>().unwrap();
    let edge_cnt = std::env::args().skip(2).next().unwrap().parse::<usize>().unwrap();

    timely::execute_from_args(std::env::args().skip(2), move |root| {

        let index = root.index() as usize;
        let peers = root.peers() as usize;

        let start = time::precise_time_s();

        // let mut segments = SegmentList::new(1024); // list of edge segments
        let mut sorter = Sorter::new();

        let mut src = vec![];   // holds ranks
        let mut deg = vec![];   // holds source degrees
        let mut rev = vec![];   // holds (dst, deg) pairs
        let mut trn = vec![];   // holds transposed sources

        let mut going = start;

        let mut input = root.dataflow(|builder| {

            let (input, edges) = builder.new_input::<(u32, u32)>();
            let (cycle, ranks) = builder.loop_variable::<(u32, f32)>(20, 1);

            edges.binary_notify(&ranks,
                                Exchange::new(|x: &(u32,u32)| x.0 as u64),
                                Exchange::new(|x: &(u32,f32)| x.0 as u64),
                                "pagerank",
                                vec![RootTimestamp::new(0)],
                                move |input1, input2, output, notificator| {

                // receive incoming edges (should only be iter 0)
                input1.for_each(|_iter, data| {
                    for &(src,dst) in data.iter() {
                        sorter.push((src,dst), &|&(_,d)| d);
                    }
                });

                // all inputs received for iter, commence multiplication
                notificator.for_each(|iter,_,_| {

                    let now = time::now();

                    if index == 0 { println!("{}:{}:{}.{} starting iteration {}", now.tm_hour, now.tm_min, now.tm_sec, now.tm_nsec, iter.inner); }

                    // if the very first iteration, prepare some stuff.
                    // specifically, transpose edges and sort by destination.
                    if iter.inner == 0 {
                        let temp = sorter.finish(&|&(_,d)| d);
                        // if index == 0 { println!("edges: {:?}", temp); }
                        let (a, b, c) = transpose(temp, peers, node_cnt);
                        deg = a; rev = b; trn = c;
                        src = vec![0.0f32; deg.len()];
                    }

                    // record some timings in order to estimate per-iteration times
                    if iter.inner == 0  && index == 0 { println!("src: {}, dst: {}, edges: {}", src.len(), rev.len(), trn.len()); }
                    if iter.inner == 10 && index == 0 { going = time::precise_time_s(); }
                    if iter.inner == 20 && index == 0 { println!("average: {}", (time::precise_time_s() - going) / 10.0 ); }

                    // prepare src for transmitting to destinations
                    for s in 0..src.len() { src[s] = 0.15 + 0.85 * src[s] / deg[s] as f32; }

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
                    notificator.notify_at(iter.retain());
                    for &(node, rank) in data.iter() {
                        src[node as usize / peers] += rank;
                    }
                });
            }).connect_loop(cycle);

            input
        });

        let seed: &[_] = &[1, 2, 3, index];
        let mut rng: StdRng = SeedableRng::from_seed(seed);

        for _index in 0..(edge_cnt / peers) {
            input.send((rng.gen_range(0, node_cnt as u32), rng.gen_range(0, node_cnt as u32)));
        }
    }).unwrap();
}

// returns [src/peers] degrees, (dst, deg) pairs, and a list of [src/peers] endpoints
fn transpose(edges: Vec<Vec<(u32, u32)>>, peers: usize, nodes: usize) -> (Vec<u32>, Vec<(u32, u32)>, Vec<u32>)  {

    let mut deg = vec![0; (nodes as usize / peers) + 1];
    for list in &edges {
        for &(s, _) in list {
            deg[s as usize / peers] += 1;
        }
    }

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
