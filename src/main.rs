extern crate mmap;
extern crate time;
extern crate timely;
extern crate columnar;
extern crate docopt;

use docopt::Docopt;

use std::thread;

use timely::progress::timestamp::RootTimestamp;
use timely::progress::scope::Scope;
use timely::progress::nested::Summary::Local;
use timely::example_shared::*;
use timely::example_shared::operators::*;
use timely::communication::*;
use timely::communication::pact::Exchange;

use timely::networking::initialize_networking;
use timely::networking::initialize_networking_from_file;

use timely::drain::DrainExt;

use columnar::Columnar;

mod typedrw;
mod graphmap;
use graphmap::GraphMMap;

mod sorting;
use sorting::{SegmentList, radix_sort_32};

static USAGE: &'static str = "
Usage: pagerank <source> [options] [<arguments>...]

Options:
    -w <arg>, --workers <arg>    number of workers per process [default: 1]
    -p <arg>, --processid <arg>  identity of this process      [default: 0]
    -n <arg>, --processes <arg>  number of processes involved  [default: 1]
    -h <arg>, --hosts <arg>      file containing list of host:port for workers
";

#[derive(Copy, Clone, Debug)]
struct NodeRank { node: u32, rank: f32, }
impl Columnar for NodeRank { type Stack = Vec<NodeRank>; }

fn main () {
    let args = Docopt::new(USAGE).and_then(|dopt| dopt.parse()).unwrap_or_else(|e| e.exit());

    let source = args.get_str("<source>").to_owned();

    let workers: u64 = if let Ok(threads) = args.get_str("-w").parse() { threads }
                       else { panic!("invalid setting for --workers: {}", args.get_str("-t")) };
    let process_id: u64 = if let Ok(proc_id) = args.get_str("-p").parse() { proc_id }
                          else { panic!("invalid setting for --processid: {}", args.get_str("-p")) };
    let processes: u64 = if let Ok(processes) = args.get_str("-n").parse() { processes }
                         else { panic!("invalid setting for --processes: {}", args.get_str("-n")) };

    println!("Starting pagerank dataflow with");
    println!("\tworkers:\t{}", workers);
    println!("\tprocesses:\t{}", processes);
    println!("\tprocessid:\t{}", process_id);

    // vector holding communicators to use; one per local worker.
    if processes > 1 {
        println!("Initializing BinaryCommunicator");

        let hosts = args.get_str("-h");
        let communicators = if hosts != "" {
            initialize_networking_from_file(hosts, process_id, workers).ok().expect("error initializing networking")
        }
        else {
            let addresses = (0..processes).map(|index| format!("localhost:{}", 2101 + index).to_string()).collect();
            initialize_networking(addresses, process_id, workers).ok().expect("error initializing networking")
        };

        pagerank_spawn(communicators, source);
    }
    else if workers > 1 { pagerank_spawn(ProcessCommunicator::new_vector(workers), source); }
    else { pagerank_spawn(vec![ThreadCommunicator], source); };
}

fn pagerank_spawn<C>(communicators: Vec<C>, filename: String)
where C: Communicator+Send {
    let mut guards = Vec::new();
    let workers = communicators.len();
    for communicator in communicators.into_iter() {
        let filename = filename.clone();
        guards.push(thread::Builder::new().name(format!("timely worker {}", communicator.index()))
                                          .spawn(move || pagerank_thread(communicator, filename, workers))
                                          .unwrap());
    }

    for guard in guards { guard.join().unwrap(); }
}

// returns [src/peers] degrees, (dst, deg) pairs, and a list of [src/peers] endpoints
fn transpose(mut edges: Vec<Vec<(u32, u32)>>, peers: usize, nodes: usize) -> (Vec<u32>, Vec<(u32, u32)>, Vec<u32>)  {

    let mut deg = vec![0; (nodes as usize / peers) + 1];
    for list in &edges {
        for &(s, _) in list {
            deg[s as usize / peers] += 1;
        }
    }

    // let start = time::precise_time_s();
    radix_sort_32(&mut edges, &mut Vec::new(), &|&(_,d)| d);
    // println!("sorted in {}s", time::precise_time_s() - start);

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

// pagerank dataflow graph has a set of edges as input, and a binary vertex that for each epoch of
// received edges initiates an iterative subcomputation to compute the pagerank.

fn pagerank_thread<C>(communicator: C, filename: String, _workers: usize)
where C: Communicator {
    let index = communicator.index() as usize;
    let peers = communicator.peers() as usize;

    let mut root = GraphRoot::new(communicator);

    let start = time::precise_time_s();

    let nodes = GraphMMap::new(&filename).nodes();

    let mut segments = SegmentList::new(1024); // list of edge segments

    let mut src = vec![];   // holds ranks
    let mut deg = vec![];   // holds source degrees
    let mut rev = vec![];   // holds (dst, deg) pairs
    let mut trn = vec![];   // holds transposed sources

    let mut going = start;

    let mut input = root.subcomputation(|builder| {

        let (input, edges) = builder.new_input::<(u32, u32)>();
        let (cycle, ranks) = builder.loop_variable::<NodeRank>(RootTimestamp::new(20), Local(1));

        edges.binary_notify(&ranks,
                            Exchange::new(|x: &(u32,u32)| x.0 as u64),
                            Exchange::new(|x: &NodeRank| x.node as u64),
                            format!("pagerank"),
                            vec![RootTimestamp::new(0)],
                            move |input1, input2, output, notificator| {

            // receive incoming edges
            while let Some((_index, data)) = input1.pull() {
                segments.push(data.drain_temp());
            }

            while let Some((iter, _)) = notificator.next() {

                // if the very first iteration, prepare some stuff
                if iter.inner == 0 {
                    let (a, b, c) = transpose(segments.finalize(), peers, nodes);
                    deg = a; rev = b; trn = c;
                    src = vec![0.0f32; deg.len()];
                }

                if iter.inner == 10 && index == 0 { going = time::precise_time_s(); }
                if iter.inner == 20 && index == 0 { println!("avg: {}", (time::precise_time_s() - going) / 10.0 ); }

                // println!("{:.3}\tworker {}: notify[{}] begin", time::precise_time_s() - start, index, iter.inner);

                for s in 0..src.len() { src[s] = 0.15 + 0.85 * src[s] / deg[s] as f32; }

                let mut trn_slice = &trn[..];
                let mut rev_slice = &rev[..];
                while rev_slice.len() > 0 {
                    // TODO: session should just flush...
                    let mut session = output.session(&iter);
                    let next = std::cmp::min(200_000, rev_slice.len());
                    for &(dst, deg) in &rev_slice[..next] {
                        let mut accum = 0.0;
                        for &s in &trn_slice[..deg as usize] {
                            accum += src[s as usize];
                        }
                        trn_slice = &trn_slice[deg as usize..];
                        session.give(NodeRank { node: dst, rank: accum });
                    }
                    rev_slice = &rev_slice[next..];
                }

                for s in &mut src { *s = 0.0; }

                // println!("{:.3}\tworker {}: notify[{}] ended", time::precise_time_s() - start, index, iter.inner);
            }

            while let Some((iter, data)) = input2.pull() {
                notificator.notify_at(&iter);
                for x in data.drain_temp() {
                    src[x.node as usize / peers] += x.rank;
                }
            }
        })
        .connect_loop(cycle);

        input
    });


    {
        let graph = GraphMMap::new(&filename);

        let mut edges = Vec::new();
        for node in 0..graph.nodes() {
            if node % peers == index {
                for dst in graph.edges(node) {
                    edges.push((node as u32, *dst as u32));
                }
                if edges.len() > 100_000 {
                    input.send_at(0, edges.drain_temp());
                    root.step();
                }
            }
        }


        input.send_at(0, edges.drain_temp());
    }
    input.close();
    while root.step() { }

    if index == 0 { println!("elapsed: {}", time::precise_time_s() - start); }
}



    // .unary_notify(
    //     Exchange::new(move |x: &(u32, f32)| (workers * (index / workers)) as u64 + (x.0 as u64 % workers as u64)),
    //     format!("Aggregation"),
    //     vec![],
    //     move |input, output, iterator| {
    //         while let Some((iter, data)) = input.pull() {
    //             iterator.notify_at(&iter);
    //             for (node, rank) in data.drain_temp() {
    //                 acc[node as usize / workers] += rank;
    //             }
    //         }
    //
    //         while let Some((item, _)) = iterator.next() {
    //
    //             output.give_at(&item, acc.drain_temp().enumerate().filter(|x| x.1 != 0.0)
    //                                      .map(|(u,f)| (((u * workers + local_index) as u32), f)));
    //
    //             for _ in 0..(1 + (nodes/workers)) { acc.push(0.0); }
    //             assert!(acc.len() == (1 + (nodes/workers)));
    //         }
    //     }
    // )
