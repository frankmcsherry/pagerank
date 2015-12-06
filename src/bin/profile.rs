extern crate rand;
extern crate mmap;
extern crate time;
extern crate timely;
extern crate getopts;

use rand::{Rng, SeedableRng, StdRng};

use timely::progress::timestamp::RootTimestamp;
use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::channels::pact::Exchange;
use timely::drain::DrainExt;

fn main () {

    let node_cnt = std::env::args().skip(1).next().unwrap().parse::<usize>().unwrap();
    let edge_cnt = std::env::args().skip(2).next().unwrap().parse::<usize>().unwrap();

    timely::execute_from_args(std::env::args(), move |root| {

        let index = root.index() as usize;
        let peers = root.peers() as usize;

        let start = time::precise_time_s();

        let mut edges = Vec::new();
        let mut ranks = vec![1.0; node_cnt];   // holds ranks
        if (node_cnt % peers) < index { ranks.push(1.0); }
        let mut degrs = vec![0; ranks.len()];

        let mut going = start;

        let mut input = root.scoped(|builder| {

            let (input, graph) = builder.new_input::<(u32, u32)>();
            let (cycle, loopz) = builder.loop_variable::<(u32, f32)>(20, 1);

            graph.binary_notify(&loopz,
                                Exchange::new(|x: &(u32,u32)| x.0 as u64),
                                Exchange::new(|x: &(u32,f32)| x.0 as u64),
                                "pagerank",
                                vec![RootTimestamp::new(0)],
                                move |input1, input2, output, notificator| {

                // receive incoming edges (should only be iter 0)
                while let Some((_index, data)) = input1.next() {
                    for (src,dst) in data.drain_temp() {
                        degrs[src as usize] += 1;
                        edges.push((src,dst));
                    }
                }

                // all inputs received for iter, commence multiplication
                while let Some((iter, _)) = notificator.next() {

                    // record some timings in order to estimate per-iteration times
                    if iter.inner == 0 { println!("src: {}, dst: {}, edges: {}", ranks.len(), node_cnt, edges.len()); }
                    if iter.inner == 10 && index == 0 { going = time::precise_time_s(); }
                    if iter.inner == 20 && index == 0 { println!("average: {}", (time::precise_time_s() - going) / 10.0 ); }

                    // prepare src for transmitting to destinations
                    for s in 0..ranks.len() { ranks[s] = 0.15 + 0.85 * ranks[s] / degrs[s] as f32; }

                    // wander through destinations
                    let mut session = output.session(&iter);
                    for &(src,dst) in &edges {
                        unsafe {
                            session.give((dst, *ranks.get_unchecked(src as usize)));
                        }
                    }

                    for s in &mut ranks { *s = 0.0; }
                }

                // receive data from workers, accumulate in src
                while let Some((iter, data)) = input2.next() {
                    notificator.notify_at(&iter);
                    for &(node, rank) in data.iter() {
                        unsafe { *ranks.get_unchecked_mut(node as usize / peers) += rank; }
                    }
                }
            }).connect_loop(cycle);

            input
        });


        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng: StdRng = SeedableRng::from_seed(seed);

        for _index in 0..(edge_cnt / peers) {
            input.send((rng.gen_range(0, node_cnt as u32), rng.gen_range(0, node_cnt as u32)));
        }
    }); 
}