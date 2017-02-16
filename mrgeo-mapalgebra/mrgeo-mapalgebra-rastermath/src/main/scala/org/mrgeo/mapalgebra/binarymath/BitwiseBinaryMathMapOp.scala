/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.mapalgebra.binarymath

import org.apache.spark.SparkContext
import org.mrgeo.mapalgebra.parser.ParserException

abstract class BitwiseBinaryMathMapOp extends RawBinaryMathMapOp {

  val EPSILON:Double = 1e-12

  override def execute(context:SparkContext):Boolean = {
    varA match {
      case Some(a) => {
        val metadata = a.metadata().getOrElse(throw new ParserException("Uh oh - no metadata available"))
        if (metadata.isFloatingPoint) {
          log.warn(
            "Using a floating point raster like " + metadata.getPyramid + " is not recommended for bitwise operators")
        }
      }
      case None => {}
    }
    constA match {
      case Some(a) => {
        val remainder = a % 1
        if (remainder < -EPSILON || remainder > EPSILON) {
          log.warn("Using floating point values in bitwise operators is not recommended")
        }
      }
      case None => {}
    }
    varB match {
      case Some(b) => {
        val metadata = b.metadata().getOrElse(throw new ParserException("Uh oh - no metadata available"))
        if (metadata.isFloatingPoint) {
          log.warn(
            "Using a floating point raster like " + metadata.getPyramid + " is not recommended for bitwise operators")
        }
      }
      case None => {}
    }
    constB match {
      case Some(b) => {
        val remainder = b % 1
        if (remainder < -EPSILON || remainder > EPSILON) {
          log.warn("Using floating point values in bitwise operators is not recommended")
        }
      }
      case None => {}
    }
    return super.execute(context)
  }

}
