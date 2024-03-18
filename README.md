# Aggregation of Out-Of-Order Stream
This is the implementation of the paper "O(1)-Time Complexity for Fixed Sliding-window Aggregation over Out-of-order Data Streams".

## Contact
savong-hashimoto@cs.tsukuba.ac.jp

## Input:
Can accept data streams from:
* Socket
* Apache Kafka
* and others

  
### Dataset
* [A DEBS12 Grand Challenge](https://debs.org/grand-challenges/2012/)
* [Electricity dataset](https://paperswithcode.com/dataset/electricity)

### Out-of-order streams
Out-of-order streams are simulated according to the [method](https://github.com/TU-Berlin-DIMA/out-of-order-datagenerator) mentioned in the paper "[Generating Reproducible Out-of-Order Data Streams](https://dl.acm.org/doi/10.1145/3328905.3332511)" in DEBS '19: Proceedings of the 13th ACM International Conference on Distributed and Event-based Systems:  

* (1) Timestamps are attached to all records in the dataset in order if the records do not have timestamps,
* (2) To make the late records of the out-of-order streams uniformly distribute, the records are uniformly selected and their timestamps are subtracted small or big back to the past depending on how much delay is needed.

There are two main parameters for the simulation of out-of-order streams:
* Out-of-order ratio (outOfOrder):
  * The ratio of out-of-order records in the dataset
* minDelay/maxDelay:
  * Indicates the delays are uniformly distributed between the minimal and maximal delay

## Output
Stream of aggregating results.

## Parameters:
* Window size
* Slide size
* Maximum allowed lateness
* Aggregating functions must be commutative due to the out-of-order of data streams:
  * Distributive (min, max, sum, count, product, sum of squares, etc)
  * Algebraic (average, standard deviation, geometric mean, etc)
* Input rate: can be controlled from the input sources.

## Splitting Method:
Any splitting methods can be used to divide the window into partitions based on the slide:
* [Panes](https://dl.acm.org/doi/10.1145/1058150.1058158)
* [Pairs](https://dl.acm.org/doi/10.1145/1142473.1142543)
* [Scotty](https://dl.acm.org/doi/10.1145/3433675)
* [Cutty](https://dl.acm.org/doi/abs/10.1145/2983323.2983807)
* Others

[Cutty](https://dl.acm.org/doi/abs/10.1145/2983323.2983807) is adopted in this implementation.

## Main Methods:
[FoldFunction](https://nightlies.apache.org/flink/flink-docs-release-1.7/api/java/org/apache/flink/api/common/functions/FoldFunction.html) in Apache Flink is used to customize the window operator so that incremental computation based on the paper "O(1)-Time Complexity for Fixed Sliding-window Aggregation over Out-of-order Data Streams" be efficiently processed. 

## Library:
[Apache Flink DataStream API](https://flink.apache.org/) is needed.
