# Aggregation of Out-Of-Order Stream
This is the implementation of the paper "O(1)-Time Complexity for Fixed Sliding-window Aggregation over Out-of-order Data Streams".

## Contact
savong-hashimoto@cs.tsukuba.ac.jp

## Input:
Can accept data streams from:
* Socket
* Apache Kafka
* and others

## Source code and programming structure:
We have implemented the following operators:
* Minimum
* Maximum
* Sum
* Average
* Standard deviation (STD)
* Range
Other distributive and algebraic operators can be similarly implemented.

The codes can be found in:
* /Flink_Version : For the implementation using Apache Flink.
  * CMiX_PWiX_AVERAGE_using_Flink.java
  * CMiX_PWiX_MAX_using_Flink.java
  * CMiX_PWiX_MIN_using_Flink.java
  * CMiX_PWiX_RANGE_using_Flink.java
  * CMiX_PWiX_STD_using_Flink.java
  * CMiX_PWiX_SUM_using_Flink.java
* /Non-Flink_Version : For the implementation using Java without Apache Flink.
  * CMiX_PWiX_AVERAGE.java
  * CMiX_PWiX_MAX.java
  * CMiX_PWiX_MIN.java
  * CMiX_PWiX_RANGE.java
  * CMiX_PWiX_STD.java
  * CMiX_PWiX_SUM.java
The programming structures of all classes (CMiX_PWiX_AVERAGE.java, CMiX_PWiX_MAX.java, ...) are the same except that they have different operators. The main structure of each classe can be defined by the following main methods:
* "main" method:
  * Same as Algorithm 1 in the paper.
  * It requests the data stream source to access the data streams and does all processing of stream aggregation.
* "Compute_Result" method:
  * Same as Algorithm 2 in the paper.
  * It computes the aggregating results of the current window.
* "Aggkplus1Block" method:
  * Same as Algorithm 3 in the paper.
  * It does backward aggregation from the right-most to the left-most partitions in the next block.
* "UpdatePCResult":
  * Same as Algorithm 4 in the paper.
  * It maintains the aggregating results of the current and past windows within the maximum allowed lateness.
* "UPResult" method:
  * Same as Algorithm 5 in the paper.
  * The affected partitions and other partitions with newer timestamps are updated by aggregating the late-arrival records into them.
 

## How to run the program: 
To run the program, it is required to build a virtual streaming environment. by using to programs
* Data Stream Source: A program to read data from file line by line and send to the requested "Operator program".
* Operator program: A program to request accessing data streams from the "Data Stream Source". After accessing the data streams, various computations can be applied.

* Java  /DataStreamSource/DataStreamSource_Transmission.java
* Java  /Non-Flink_Version/CMiX_PWiX_MAX.java
  
  
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
