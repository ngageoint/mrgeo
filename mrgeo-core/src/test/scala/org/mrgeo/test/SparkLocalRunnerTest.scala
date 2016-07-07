/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

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

    sparkConf.setMaster("local")
    sparkConf.setAppName("Test App")

    sparkContext = SparkContext.getOrCreate(sparkConf)
  }

  @After
  def teardown(): Unit = {
    if (sparkContext != null) {
      sparkContext.stop()
    }
  }
}
