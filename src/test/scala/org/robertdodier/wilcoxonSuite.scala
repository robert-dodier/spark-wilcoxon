package org.robertdodier

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.{SparkConf, SparkContext}

class wilcoxonSuite extends FunSuite with BeforeAndAfterAll {

  @transient var sc: SparkContext = _

  override def beforeAll() {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
    sc = new SparkContext(conf)
    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }

  test("compute scaled rank sum with made-up data") {
    val scoresAndLabels = Seq((0.6, 1), (0.9, 1), (0.3, 1), (0.2, 0), (0.1, 0), (0.5, 0))
    val U = wilcoxon.U (sc.parallelize (scoresAndLabels))
    val expected_U = 8.0/9.0
    assert (Math.abs (U - expected_U) <= 1e-12)
  }
}
