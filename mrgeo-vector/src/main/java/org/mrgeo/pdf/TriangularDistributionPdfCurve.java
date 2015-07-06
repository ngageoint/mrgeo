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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;

import java.awt.geom.Point2D;
import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class TriangularDistributionPdfCurve implements PdfCurve
{
  private static double EPSILON = 1e-8;

  //@SuppressWarnings("unused")
  private double _max = Double.NaN;
  private double _min = Double.NaN;
  private double _bin = Double.NaN;
  private double _mode = Double.NaN;
  private double[] _likelihoods;
  
  public TriangularDistributionPdfCurve()
  {
  }
  
  public TriangularDistributionPdfCurve(double min, double mode, double max, double bin)
  {
    _min = min;
    _mode = mode;
    _max = max;
    _bin = bin;
  }

  public void writeMetadata(Path output, Configuration conf) throws IOException
  {
    FSDataOutputStream os = null;
    try
    {
      FileSystem dfs = output.getFileSystem(conf);
      os = dfs.create(new Path(output, "metadata"));
      os.writeUTF(TriangularDistributionPdfCurve.class.getName());
      
      os.writeInt(1); // version
      os.writeDouble(_min);
      os.writeDouble(_max);
      os.writeDouble(_bin);
      os.writeDouble(_mode);
    }
    finally
    {
      IOUtils.closeStream(os);
    }
  }
  
  public void writePdfCurve(Path output, Configuration conf) throws IOException
  {
    FileSystem fs = output.getFileSystem(conf);
    Path outputFile = new Path(output, "part-r-00000");
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, outputFile,
        DoubleWritable.class, DoubleWritable.class);
    
    DoubleWritable key = new DoubleWritable();
    DoubleWritable value = new DoubleWritable();
  
    double resolution = (_max - _min)/_bin;
    double binNumber = _min;
    for (int i = 0; i < _bin; i++)
    {
      double likelihood = getLikelihood(binNumber);
      key.set(binNumber);
      value.set(likelihood);
      writer.append(key, value);
      binNumber += resolution;
    }

    writer.close();
  }
  
  public static PdfCurve load(DataInput metadataInputStream, Path[] pdfFiles, Configuration conf) throws IOException
  {
    int version = metadataInputStream.readInt(); // version
    if (version == 1)
    {
      TriangularDistributionPdfCurve pdfCurve = new TriangularDistributionPdfCurve();
      // Load the metadata
      pdfCurve._min = metadataInputStream.readDouble();
      pdfCurve._max = metadataInputStream.readDouble();
      pdfCurve._bin = metadataInputStream.readDouble();
      pdfCurve._mode = metadataInputStream.readDouble();
      // Compute the curve using the PDF histogram stored at pdfPath. The histogram is written
      // out by PdfDriver and PdfHistogramBuilder
      pdfCurve._computeCurve(pdfFiles, conf);
      return pdfCurve;
    }
    throw new IOException("Invalid version: " + version);
  }
  
  /* (non-Javadoc)
   * @see org.mrgeo.ml.classifier.PdfCurve#getLikelihood(double)
   */
  @Override
  public double getLikelihood(double rfdValue)
  {
    double likelihood = 0.0;
    if (_max <= _min + EPSILON)
    {
      throw new IllegalArgumentException(String.format(
          "Invalid PDF curve, max (%f) must be greater than min (%f)",
          _max, _min));
    }
    if ((_mode < _min - EPSILON) || (_mode > _max + EPSILON))
    {
      throw new IllegalArgumentException(String.format(
          "Invalid PDF curve, mode (%f) must fall between min (%f) and max (%f)",
          _mode, _min, _max));
    }
    if (!Double.isNaN(_min) && !Double.isNaN(_max) && !Double.isNaN(_mode) && !Double.isNaN(_bin))
    {
      if ((rfdValue < _min - EPSILON) || (rfdValue > _max + EPSILON))
      {
        return 0;
      }
     
      if (_mode > _min + EPSILON && _mode < _max - EPSILON)
      {
        if (rfdValue >= _min - EPSILON && rfdValue <= _mode + EPSILON)
        {
          likelihood = (2 * (rfdValue - _min))/((_max - _min) * (_mode - _min));
        }
        else if (rfdValue > _mode - EPSILON && rfdValue <= _max + EPSILON)
        {
          likelihood = (2 * (_max - rfdValue))/((_max - _min) * (_max - _mode));
        }
      }
      else
      {
        likelihood = 2.0 * (rfdValue - _min) / (_max - _min);
      }
    }
   
    return likelihood;
  }

  private void _computeCurve(Path[] pdfFiles, Configuration conf) throws IOException
  {
    SequenceFile.Reader r = null;
    _likelihoods = new double[(int)_bin];
    int index = 0;
    try
    {
      // Loop through each of the output files from the reduce to process all of
      // the PDF histogram bins
      for (Path pdfFile : pdfFiles)
      {
        // ignore all the non-part files
        if (!pdfFile.getName().startsWith("part"))
        {
          continue;
        }
        r = new SequenceFile.Reader(pdfFile.getFileSystem(conf), pdfFile, conf);
        DoubleWritable key = new DoubleWritable();
        DoubleWritable value = new DoubleWritable();
        while (r.next(key, value))
        {
          _likelihoods[index] = value.get();
          index++;
        }
      }
    }
    finally
    {
      IOUtils.closeStream(r);
    }
  }

  @Override
  public void export(OutputStream os, String format) throws IOException
  {
    if (Double.isNaN(_min) || Double.isNaN(_max) || Double.isNaN(_mode) || Double.isNaN(_bin))
    {
      throw new IOException("PDF not exported. Need to setup min, max, mode or bin.");
    }
    double resolution = (_max - _min)/_bin;
    PrintWriter w = new PrintWriter(os);
    double rfdValue = _min;
    while (rfdValue < _max)
    {
      w.println(rfdValue + ", " + getLikelihood(rfdValue));
      rfdValue += resolution;
    }
    // The flush is required here because it is not guaranteed
    // when the caller closes the passed in OutputStream that it
    // will be flushed properly.
    w.flush();
  }

  @Override
  public List<Point2D.Double> getPoints() throws IOException
  {
    if (Double.isNaN(_min) || Double.isNaN(_max) || Double.isNaN(_mode) || Double.isNaN(_bin))
    {
      throw new IOException("Unable to return curve points. Need to setup min, max, mode or bin.");
    }
    double resolution = (_max - _min)/_bin;
    ArrayList<Point2D.Double> results = new ArrayList<Point2D.Double>();
    double rfdValue = _min;
    while (rfdValue < _max)
    {
      Point2D.Double p = new Point2D.Double(rfdValue, getLikelihood(rfdValue));
      results.add(p);
      rfdValue += resolution;
    }
    return results;
  }

  public void setBin(double bin)
  {
    _bin = bin;
  }
  
  public void setMin(double min)
  {
    _min = min;
  }
  
  public void setMax(double max)
  {
    _max = max;
  }
  
  public void setMode(double mode)
  {
    _mode = mode;
  }

  @Override
  public double diff(PdfCurve otherCurve)
  {
    if (!(otherCurve instanceof TriangularDistributionPdfCurve))
    {
      throw new IllegalArgumentException("Cannot diff a " +
          this.getClass().getSimpleName() +
          " " + otherCurve.getClass().getSimpleName());
    }
    return Math.abs(area() - ((TriangularDistributionPdfCurve)otherCurve).area());
  }

  @Override
  public double area()
  {
    double modeLikelihood = getLikelihood(_mode);
    return (_max - _min) * modeLikelihood / 2.0;
  }
}
