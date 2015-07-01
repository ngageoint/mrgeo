/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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
 */

package org.mrgeo.utils

object Memory {

  def used(): String = {
    val bytes = Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()

    format(bytes)
  }

  def free():String = {
    format(Runtime.getRuntime.freeMemory)
  }

  def allocated():String = {
    format(Runtime.getRuntime.totalMemory())
  }

  def total():String = {
    format(Runtime.getRuntime.maxMemory())
  }


  def format(bytes: Long): String = {
    val unit = 1024
    if (bytes < unit) {
      return bytes + "B"
    }
    val exp = (Math.log(bytes) / Math.log(unit)).toInt
    val pre = "KMGTPE".charAt(exp - 1)

    "%.1f%sB".format(bytes / Math.pow(unit, exp), pre)
  }
}
