### Definition

spark-wilcoxon computes the Wilcoxon-Mann-Whitney rank sum statistic
(also known by other permutations of names) for data `X` which fall
into two classes, here denoted class 1 and class 0. The statistic
is defined as

  `U = sum(R1[i], i=1, ..., n1) - n1*(n1 + 1)/2`

where `n1` is the number of data in class 1 and `R1` are the ranks of
the data in class 1, among all the data, with the least value having the
lowest rank and the greatest value having the highest rank. The least
rank of all is 1 and the greatest rank of all is `n`, where `n` is the
number of data of both classes.

Let `n0` be the number of data in class 0. Note that `U` is maximized
(with value `n1*n0`) when all of data in class 1 are greater than all of
the data in class 0, and minimized (with value 0) when all of the data
in class 0 are greater than all of the data in class 1. The scaled
statistic `U/(n1*n0)` therefore ranges from 0 to 1.

The expected value of `U`, when class 1 and class 0 have the same
distribution, is `n1*n0/2`. In that case the scaled statistic has the
value 1/2.

### Implementation

spark-wilcoxon defines an object `wilcoxon` which has two methods, both
of which compute `U/(n1*n0)`.

- `wilcoxon.U(RDD[(Double, Int)])`: the sole argument is an RDD
comprising pairs of data and class labels.
- `wilcoxon.allU(RDD[LabeledPoint])`: the sole argument is an RDD of
labeled points. The label is the class label and the features are data
values. This method returns a sequence of computed statistics, one for
each feature.

### Applications

The scaled rank sum statistic, `U/(n1*n0)`, is equivalent to the area
under the ROC curve, considering the data as scores for the binary
classification problem of distinguishing class 1 from class 0. As such,
it is a fast, simple way to assess the relevance of the data for the
binary classification problem. Scaled values near 1 or 0 indicated
greater relevance (with 1 indicating positive correlation and 0
indicating negative correlation), and 1/2 indicating irrelevance.

### Building spark-wilcoxon

spark-wilcoxon uses SBT.

- `sbt compile` to compile the Wilcoxon code
- `sbt test` to compile and execute tests
- `sbt assembly` to produce a jar

### Running spark-wilcoxon

Execute `sbt assembly` to produce a jar, which can be used by a stand-
alone program, or by `spark-shell` or other interactive environment.

Here is an example using `spark-shell`. The object `wilcoxon` contains
a method `run_example` which constructs a two-class problem in which
the classes are represented by Gaussian bumps which each have unit
variance and means which differ by a multiple of 1/2. This example shows
how the Wilcoxon statistic is 1/2 when the bumps are indistinguishable,
and nearer to 1 or 0 as the bumps become more distinct.

```
$ spark-shell -classpath ./target/scala-2.10/spark-wilcoxon_2.10.6-0.0.1-SNAPSHOT-ASSEMBLY.jar
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.5.1
      /_/

Using Scala version 2.10.4 (Java HotSpot(TM) Server VM, Java 1.8.0_60)
Type in expressions to have them evaluated.
Type :help for more information.
Spark context available as sc.
SQL context available as sqlContext.

scala> import org.robertdodier.wilcoxon
import org.robertdodier.wilcoxon

scala> wilcoxon.run_example (sc)
difference of means = 0.0; U/(n1*n0) = 0.50723                                  
difference of means = 0.5; U/(n1*n0) = 0.635793
difference of means = 1.0; U/(n1*n0) = 0.762139                                 
difference of means = -0.5; U/(n1*n0) = 0.366783
difference of means = -1.0; U/(n1*n0) = 0.234772
difference of means = -1.5; U/(n1*n0) = 0.142386

scala>
```
