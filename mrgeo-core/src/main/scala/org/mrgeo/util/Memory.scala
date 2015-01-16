package org.mrgeo.util

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
