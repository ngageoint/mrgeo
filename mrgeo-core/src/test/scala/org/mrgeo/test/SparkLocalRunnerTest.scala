package org.mrgeo.test

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before}

class SparkLocalRunnerTest
{
  var sparkConf: SparkConf = null
  var sparkContext: SparkContext = null

  @Before
  def initLocalRunner(): Unit = {
    sparkConf = new SparkConf()
    sparkContext = new SparkContext("local", "Test App", sparkConf)
  }

  @After
  def teardown(): Unit = {
    if (sparkContext != null) {
      sparkContext.stop()
    }
  }
}
