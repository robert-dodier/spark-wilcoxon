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
  }
}
