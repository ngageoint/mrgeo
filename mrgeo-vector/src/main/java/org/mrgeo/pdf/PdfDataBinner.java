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

package org.mrgeo.pdf;


import org.mrgeo.databinner.DataBinner;

/**
 * Computes a PDF bin for an RFD value. This is based on the Signature
 * Analyst NoLimitOrionPdfCurve algorithm for binning RFD data.
 */
public class PdfDataBinner implements DataBinner
{
  // The number of bandwidths to extend the range of the PDF curve beyond
  // the actual min/max of the RFD data.
  private static final int PDF_RANGE_CONSTANT = 4;
  private static final double NUM_SAMPLES_PER_BW = 12.0;

  private double _max;
  private double _min;
  private double _resolution;
  private double _bw;
  private double _lambda;

  public PdfDataBinner(double min, double max, double minBw, long dataSize, double q1, double q3)
  {
    _lambda = q3 - q1;
    if (dataSize <= 0)
    {
      _bw = -1;
    }
    else
    {
      _bw = 0.79 * _lambda * Math.pow(dataSize, -1.0 / 5.0);
      _bw = Math.max(_bw, minBw);
//      _min = min - ((double)PDF_RANGE_CONSTANT * _bw);
      // TODO: Verify with Eugene that the min is always 0 for FTF's that use distance
      _min = 0.0;
      _max = max + (PDF_RANGE_CONSTANT * _bw);
      _resolution = _bw / NUM_SAMPLES_PER_BW;
    }
  }

  @Override
  public int calculateBin(double value)
  {
    // safeguard
    if (_bw <= 0.0)
    {
      return 0;
    }
    if (value < _min || value > _max)
    {
      return -1;
    }
    double calcBin = (value - _min) / _resolution;
    return (int)calcBin;
  }
  
  @Override
  public int getBinCount()
  {
    return (int)((_max - _min) / _resolution) + 1;
  }

  public double getResolution()
  {
    return _resolution;
  }
}
