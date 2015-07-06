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

package org.mrgeo.databinner;


import java.io.Serializable;

/**
 * Bins are equal sized, computed by dividing the range of values
 * by the number of bins. If an instance is configured with binZeroForNan
 * set to true, then the 0 bin is reserved for NaN values, while
 * non-NaN values are divided between bins 1 .. numBins. If that flag is
 * false, then all values will be distributed between bins 0 through
 * numBins, and NaN values will fall into bin 0.
 */
public class EqualSizeDataBinner implements DataBinner, Serializable
{
  private static final long serialVersionUID = 1L;
  private boolean _binZeroForNan;
  private int _numBins;
  private double _min;
  private double _max;

  /**
   * Disable use of the default constructor.
   */
  @SuppressWarnings("unused")
  private EqualSizeDataBinner()
  {
  }

  /**
   * Use this constructor in cases where you don't know the min/max
   * up front. You must call the update() method prior to calling
   * calculateBin() in order to get reasonable results.
   * 
   * @param binZeroForNan Set this to true if only Nan values should be
   * placed into bin 0. Other values will be binned from 1..numBins. If
   * false, NaN values will still go into bin 0, but other values will
   * be split up in bins 0 thru numBins - 1.
   */
  public EqualSizeDataBinner(boolean binZeroForNan)
  {
    this._binZeroForNan = binZeroForNan;
  }

  /**
   * Use this constructor if you know the values for the parameters
   * at construction time.
   * 
   * @param binZeroForNan Set this to true if only Nan values should be
   * placed into bin 0. Other values will be binned from 1..numBins. If
   * false, NaN values will still go into bin 0, but other values will
   * be split up in bins 0 thru numBins - 1.
   * @param numBins
   * @param min
   * @param max
   */
  public EqualSizeDataBinner(boolean binZeroForNan, int numBins, double min, double max)
  {
    this._binZeroForNan = binZeroForNan;
    update(numBins, min, max);
  }

  /**
   * This method allows the caller to reuse an instance of this class after
   * the binning parameters change.
   * 
   * @param numBins
   * @param min
   * @param max
   */
  public void update(int numBins, double min, double max)
  {
    this._numBins = numBins;
    this._min = min;
    this._max = max;
  }

  /**
   * Divide the range of values (max - min) by the number of bins to determine
   * the size of each bin, and therefore which bin each value will fall into.
   * If a case arises where a value is smaller than the min, it will be placed
   * in the first bin. If a value is larger than the max, it will be placed in
   * the last bin.
   */
  @Override
  public int calculateBin(double value)
  {
//    if (_min > _max || value < _min || value > _max || Double.isNaN(value)) {
    if (_min > _max || Double.isNaN(value)) {
      return 0;
    }
    int bin = (int) ((value - _min) / (_max - _min) * _numBins);
    bin = Math.max(0, Math.min(bin, _numBins - 1));
    if (_binZeroForNan)
    {
      bin++;
    }
    return bin;
  }
  
  @Override
  public int getBinCount()
  {
    return _numBins;
  }
}
