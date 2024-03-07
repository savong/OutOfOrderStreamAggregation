# Aggregation of Out-Of-Order Stream
This is the implementation of the paper "O(1)-Time Complexity for Fixed Sliding-window Aggregation over Out-of-order Data Streams".

## Contact
savong-hashimoto@cs.tsukuba.ac.jp

## Dataset
* [A DEBS12 Grand Challenge](https://debs.org/grand-challenges/2012/)
* [Electricity dataset](https://paperswithcode.com/dataset/electricity)

## Out-of-order streams
Out-of-order streams are simulated according to the [method](https://github.com/TU-Berlin-DIMA/out-of-order-datagenerator) mentioned in the paper "[Generating Reproducible Out-of-Order Data Streams](https://dl.acm.org/doi/10.1145/3328905.3332511)" in DEBS '19: Proceedings of the 13th ACM International Conference on Distributed and Event-based Systems:  

* (1) Timestamps are attached to all records in the dataset in order if the records do not have timestamps,
* (2) To make the late records of the out-of-order streams uniformly distribute, the records are uniformly selected and their timestamps are subtracted small or big back to the past depending on how much delay is needed.

There are two main parameters for simulation of out-of-order streams:
* outOfOrder or out-of-order ratio:
  * The ratio of out-of-order records in the dataset
* minDelay/maxDelay:
  * Indicates the delays are uniformly distributed between the minimal and maximal delay

