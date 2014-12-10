/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.data.shp.util;

public class StopWatch extends java.lang.Object implements java.io.Serializable
{
  static final long serialVersionUID = 1L;
  private int binCount = -1;
  private long deltaTime = -1;
  private long startTime = -1;
  private long stopTime = -1;
  private long[] totalCount = null;
  private float[] totalTime = null;

  public StopWatch()
  {
    // nothing
  }

  public StopWatch(int binCount)
  {
    // set bin counts
    this.binCount = binCount;
    totalTime = new float[binCount];
    totalCount = new long[binCount];
  }

  public float delta()
  {
    float tempTime;
    if (deltaTime == -1)
    {
      tempTime = ((float) (System.currentTimeMillis() - startTime)) / 1000;
    }
    else
    {
      tempTime = ((float) (System.currentTimeMillis() - deltaTime)) / 1000;
    }
    deltaTime = System.currentTimeMillis();
    return tempTime;
  }

  public float delta(int bin)
  {
    if (bin < 0 || bin > binCount)
      return -1;
    float tempTime = delta();
    totalTime[bin] += tempTime;
    totalCount[bin]++;
    return tempTime;
  }

  public float elapsed()
  {
    float tempTime;
    if (stopTime == -1)
    {
      tempTime = ((float) (System.currentTimeMillis() - startTime)) / 1000;
    }
    else
    {
      tempTime = ((float) (stopTime - startTime)) / 1000;
    }
    return tempTime;
  }

  public float elapsed(int bin)
  {
    if (bin < 0 || bin > binCount)
      return -1;
    float tempTime = elapsed();
    totalTime[bin] += tempTime;
    totalCount[bin]++;
    return tempTime;
  }

  public float getAverage(int bin)
  {
    if (bin < 0 || bin > binCount)
      return -1;
    float average = (totalTime[bin] / totalCount[bin]) * 1000;
    long tempAvg = (long) average;
    average = (float) tempAvg / 1000;
    return average;
  }

  public long getCount(int bin)
  {
    if (bin < 0 || bin > binCount)
      return -1;
    return totalCount[bin];
  }

  public void resetStats()
  {
    if (binCount == -1)
      return;
    for (int i = 0; i < totalCount.length; i++)
    {
      totalTime[i] = 0;
      totalCount[i] = 0;
    }
  }

  public void restart()
  {
    resetStats();
    start();
  }

  public void start()
  {
    startTime = System.currentTimeMillis();
    stopTime = -1;
    deltaTime = -1;
  }

  public float stop()
  {
    float tempTime;
    if (startTime == -1)
    {
      tempTime = 0;
    }
    else
    {
      stopTime = System.currentTimeMillis();
      tempTime = ((float) (stopTime - startTime)) / 1000;
    }
    return tempTime;
  }

  public float stop(int bin)
  {
    if (bin < 0 || bin > binCount)
      return -1;
    float tempTime = stop();
    totalTime[bin] += tempTime;
    totalCount[bin]++;
    return tempTime;
  }
}
