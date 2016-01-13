package org.robertdodier

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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
    val m = CXXX.first ().features.size
    0 to m - 1 map (i => U (CXXX.map (p => (p.features(i), p.label.toInt))))
  }

  def main (args: Array[String]): Unit = {
    val conf = new SparkConf ().setAppName ("wilcoxon")
    val sc = new SparkContext (conf)

    run_example (sc)
  }

  def run_example (sc: SparkContext) = {
    val rng = new java.util.Random (1L)
    val data0 = for (i <- Range (0, 1000)) yield (rng.nextGaussian, 0)

    for (i <- Range (0, 3)) {
      val mean_diff = 0.5*i
      val data1 = for (i <- Range (0, 1000)) yield (mean_diff + rng.nextGaussian, 1)
      val data = sc.parallelize (data0 ++ data1)
      val myU = U (data)

      System.out.println (s"difference of means = $mean_diff; U/(n1*n0) = $myU")
    }

    for (i <- Range (0, 3)) {
      val mean_diff = - 0.5*(i + 1)
      val data1 = for (i <- Range (0, 1000)) yield (mean_diff + rng.nextGaussian, 1)
      val data = sc.parallelize (data0 ++ data1)
      val myU = U (data)

      System.out.println (s"difference of means = $mean_diff; U/(n1*n0) = $myU")
    }
  }
}
