# PageRank in timely dataflow

This repository contains an implementation of the PageRank algorithm in timely
dataflow, implemented in Rust. By default, it runs 20 PageRank iterations and
then prints some statistics.

To run, clone the repo, prepare the inputs and run. We assume that you have a 
working Rust installation, and that your input graph is in text-based edge list
format.

## Preparing the inputs

The input format for our PageRank implementation is a binary-packed adjacency
list for a graph. You can use the `parse` binary to transform an ASCII/UTF-8
edge list into this format:
```
$ cargo run --release --bin parse -- my-edgelist.txt my-graph
```
This will generate binary files `my-graph.offsets` and `my-graph.targets`,
which can be the used as inputs to the `pagerank` binary.

`parse` assumes that the inputs are in tab-seperated text, but you can change
to a different delimiter by editing `src/bin/parse.rs`. For a small example
data set that works out of the box, use the
[LiveJournal graph](https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz).
Our [blog](http://www.frankmcsherry.org/pagerank/distributed/performance/2015/07/08/pagerank.html)
[posts](http://camsas.org/blog/2015-07-08-timely-pagerank-part1.html) have links
to the larger `twitter_rv` and `uk_2007_05` data sets.

## Running PageRank
To run on inputs `my-graph.offsets` and `my-graph.targets`, run:
```
$ cargo run --release --bin pagerank -- my-graph [options]
```
Without any options, the code runs single-threadedly. The `-w` option can be
used to set the number of threads to use; `-h`,`-n` and `-p` can be used to
run distributedly:
```
$ cat hosts.txt
hostname0
hostname1
hostname2
hostname3

hostname0$ cargo run --release --bin pagerank -- my-graph -h hosts.txt -n 4 -p 0
hostname1$ cargo run --release --bin pagerank -- my-graph -h hosts.txt -n 4 -p 1
hostname2$ cargo run --release --bin pagerank -- my-graph -h hosts.txt -n 4 -p 2
hostname3$ cargo run --release --bin pagerank -- my-graph -h hosts.txt -n 4 -p 3
```
The inputs must already be present in the working directory on all hosts.

## Context

We have written [blog](http://www.frankmcsherry.org/pagerank/distributed/performance/2015/07/08/pagerank.html)
[posts](http://camsas.org/blog/2015-07-08-timely-pagerank-part1.html) about the
development of this implementation; there, we also compare it against the widely
used [GraphX system](https://spark.apache.org/graphx/) for Apache Spark.

To learn more about timely dataflow in Rust, you might be interested in the
following blog posts, too:

 * [Timely dataflow: reboot](http://www.frankmcsherry.org/dataflow/naiad/2014/12/27/Timely-Dataflow.html)
 * [Timely dataflow: core concepts](http://www.frankmcsherry.org/dataflow/naiad/2014/12/29/TD_time_summaries.html)
 * [Worst-case optimal joins, in dataflow](http://www.frankmcsherry.org/dataflow/relational/join/2015/04/11/genericjoin.html)
 * [Data-parallelism in timely dataflow](http://www.frankmcsherry.org/dataflow/relational/join/2015/04/19/data-parallelism.html)
