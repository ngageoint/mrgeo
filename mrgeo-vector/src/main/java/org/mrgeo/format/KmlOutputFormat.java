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

package org.mrgeo.format;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mrgeo.geometry.*;
import org.mrgeo.mapreduce.GeometryWritable;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.LeakChecker;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Vector;

/**
 */
public class KmlOutputFormat extends FileOutputFormat<LongWritable, GeometryWritable> implements
    Serializable
{
  private static final long serialVersionUID = 1L;

  static public class KmlRecordWriter extends RecordWriter<LongWritable, GeometryWritable>
  {
    // we initialize to avoid a bunch of if (reader != null) code. It will be recreated in the
    // initialize function.
    LineRecordReader reader = new LineRecordReader();
    Vector<String> columns = new Vector<String>();
    PrintWriter writer;
    boolean first = true;
    Path columnsOutput;

    OutputStream output;
    final boolean profile;

    public KmlRecordWriter(OutputStream output)
    {
      if (System.getProperty("mrgeo.profile", "false").compareToIgnoreCase("true") == 0)
      {
        LeakChecker.instance().add(this, ExceptionUtils.getStackTrace(new Throwable("KMLGeometryOutputFormat creation stack(ignore the Throwable...)")));
        profile = true;
      }
      else
      {
        profile = false;
      }

      this.output = output;

      writer = new PrintWriter(output);
      
      writer.append(
          "<?xml version='1.0' encoding='UTF-8'?>\n" +
          "<kml xmlns='http://www.opengis.net/kml/2.2'>\n" +
          "<Document>\n");
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException
    {
      writer.append("</Document>\n");
      writer.append("</kml>\n");
      writer.close();
      
      if (output != null)
      {
        output.close();
        output = null;
        
        if (profile)
        {
          LeakChecker.instance().remove(this);
        }
      }
    }

    @Override
    public void write(LongWritable key, GeometryWritable value) throws IOException,
        InterruptedException
    {
      write(value.getGeometry());
    }

    public void write(Geometry g) throws IOException,
        InterruptedException
    {
      if (g instanceof Polygon)
      {
        write((Polygon)g);
      }
      else if (g instanceof GeometryCollection)
      {
        for (Geometry g2 : ((GeometryCollection)g).getGeometries())
        {
          write(g2);
        }
      }
      else
      {
        // more data types can be added later
        throw new IllegalArgumentException("This data type is not supported.");
      }
    }

    public void write(Polygon value)
    {
      StringBuffer buffer = new StringBuffer();
      
      buffer.append("  <Placemark>\n");
      buffer.append("    <Polygon>\n");
      buffer.append("      <outerBoundaryIs>\n");
      buffer.append("        <LinearRing>\n");
      buffer.append("          <coordinates>\n");
      
      // write exterior ring coordinates
      LinearRing er = value.getExteriorRing();
      for (Point p : er.getPoints())
      {
        // I would like to use %g here to eliminate trailing zeros, but it doesn't appear to work 
        // in java the same way it works in C.
        buffer.append(String.format("            %.12f,%.12f,%.12f\n", p.getX(), p.getY(), p.getZ()));
      }
      
      buffer.append("          </coordinates>\n");
      buffer.append("        </LinearRing>\n");
      buffer.append("      </outerBoundaryIs>\n");

      for (int i = 0; i < value.getNumInteriorRings(); i++)
      {
        buffer.append("      <innerBoundaryIs>\n");
        buffer.append("        <LinearRing>\n");
        buffer.append("          <coordinates>\n");
        
        // write exterior ring coordinates
        LinearRing ir = value.getInteriorRing(i);
        for (Point p : ir.getPoints())
        {
          // I would like to use %g here to eliminate trailing zeros, but it doesn't appear to work 
          // in java the same way it works in C.
          buffer.append(String.format("            %.12f,%.12f,%.12f\n", p.getX(), p.getY(), p.getZ()));
        }
        
        buffer.append("          </coordinates>\n");
        buffer.append("        </LinearRing>\n");
        buffer.append("      </innerBoundaryIs>\n");
      }

      buffer.append("    </Polygon>\n");
      buffer.append("  </Placemark>\n");

      writer.append(buffer);
    }
  }

  @Override
  public RecordWriter<LongWritable, GeometryWritable> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException
  {
    Path output = getDefaultWorkFile(context, ".tsv");
    FileSystem fs = HadoopFileUtils.getFileSystem(output);
    
    KmlRecordWriter result = new KmlRecordWriter(fs.create(output));
    return result;
  }
}
