package org.mrgeo.util

object Timer
{
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()

    val nano = t1 - t0
    println("Elapsed time: %.6f".format (nano.toDouble * 1.0e-9) + " sec (" + nano + "ns)")
    result
  }

}
