# PageRank in timely dataflow

This repository contains an implementation of the PageRank algorithm in timely
dataflow, implemented in Rust. By default, it runs 20 PageRank iterations and
then prints some statistics.

To run, clone the repo and then execute (assuming a working Rust installation):
```
$ cargo run --release -- <input prefix> [options]
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

hostname0$ cargo run --release -- <input prefix> -h hosts.txt -n 4 -p 0
hostname1$ cargo run --release -- <input prefix> -h hosts.txt -n 4 -p 1
hostname2$ cargo run --release -- <input prefix> -h hosts.txt -n 4 -p 2
hostname3$ cargo run --release -- <input prefix> -h hosts.txt -n 4 -p 3
```
The inputs must already be present on all hosts.

## Input format

The input is expected to be a binary-packed adjacency list representing a
graph.

## Context

We have written a [blog post]() about the development of this implementation;
there, we also compare it against the widely used [GraphX system](https://spark.apache.org/graphx/)
for Apache Spark.

To learn more about timely dataflow in Rust, you might be interested in the
following blog posts, too:

 * [Timely dataflow: reboot](http://www.frankmcsherry.org/dataflow/naiad/2014/12/27/Timely-Dataflow.html)
 * [Timely dataflow: core concepts](http://www.frankmcsherry.org/dataflow/naiad/2014/12/29/TD_time_summaries.html)
 * [Worst-case optimal joins, in dataflow](http://www.frankmcsherry.org/dataflow/relational/join/2015/04/11/genericjoin.html)
 * [Data-parallelism in timely dataflow](http://www.frankmcsherry.org/dataflow/relational/join/2015/04/19/data-parallelism.html)
