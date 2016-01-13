package org.robertdodier

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object wilcoxon
{
  def U (XC: RDD[(Double, Int)]): Double =
  {
    val XC_sorted = XC.sortBy { case (a, b) => a }
    val foo = XC_sorted.zipWithIndex ()
    val bar = foo.map { case ((a, b), c) => (a, (b, c)) }
    val baz = bar.aggregateByKey ((0L, 0L)) ( {case ((a, b), (c, d)) => (a + 1, b + d)}, {case ((a, b), (c, d)) => (a + c, b + d)} )
    val quux = baz.map { case (a, (b, c)) => (a, c/b.toDouble) }
    val mumble = XC_sorted.join (quux)
    val blurf = mumble.filter { case (a, (b, c)) => b == 1 }
    val rank_sum = blurf.aggregate (0.0) ( {case (a, (b, (c, d))) => a + d}, {(a, b) => a + b} )
    val n = mumble.count ()
    val n1 = blurf.count ()
    val n0 = n - n1

    ((rank_sum + n1) - n1*(n1 + 1.0)/2.0)/(n1 * n0.toDouble)
  }

  def allU (CXXX : RDD[LabeledPoint]): Seq[Double] =
  {
    val C = CXXX.map (p => p.label.toInt)
    val m = CXXX.first ().features.size
    0 to m - 1 map (i => U (CXXX.map (p => p.features(i)), C))
  }

  def main (args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    val conf = new SparkConf ().setAppName ("wilcoxon")
    val sc = new SparkContext (conf)

    val X = sc.parallelize (Array (3.4, -9.3, 4.2, 17.2, 11.1, -2.7, 4.8, 9.6, -0.5, 8.2))
    val C = sc.parallelize (Array (1, 0, 1, 1, 1, 0, 0, 0, 1, 1))

    import java.lang.System
    val myU = U (X, C)
    System.out.println ("data: " + X.collect ())
    System.out.println ("class labels: " + C.collect ())
    System.out.println ("U/(n1*n0): " + myU)
  }
}
