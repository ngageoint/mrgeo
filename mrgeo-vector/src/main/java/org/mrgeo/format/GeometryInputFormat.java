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

package org.mrgeo.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.mrgeo.geometry.WellKnownProjections;
import org.mrgeo.geometryfilter.ReprojectedGeometryCollection;
import org.mrgeo.mapreduce.GeometryWritable;
import org.mrgeo.data.GeometryCollection;
import org.mrgeo.data.kml.KmlInputStream;
import org.mrgeo.data.shp.ShapefileReader;
import org.mrgeo.utils.Base64Utils;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class GeometryInputFormat extends InputFormat<LongWritable, GeometryWritable>
{
  static protected class GeometryInputSplit extends InputSplit implements Serializable
  {
    private static final long serialVersionUID = 1L;
    
    // / End index is exclusive. I.e. if start = 0 and end = 5 only 0, 1, 2, 3
    // and 4 will be processed.
    int endIndex;
    GeometryCollection gc = null;
    int startIndex;

    GeometryInputSplit(GeometryCollection gc, int start, int end)
    {
      startIndex = start;
      endIndex = end;
      this.gc = gc;
    }

    public int getEnd()
    {
      return endIndex;
    }

    @Override
    public long getLength() throws IOException
    {
      return endIndex - startIndex;
    }

    @Override
    public String[] getLocations() throws IOException
    {
      return new String[0];
    }

    public int getStart()
    {
      return startIndex;
    }
  }

  static protected class GeometryRecordReader extends RecordReader<LongWritable, GeometryWritable>
  {
    private int currentIndex;
    private int end;
    private GeometryCollection gc;
    private int start;
    private LongWritable key = new LongWritable();
    private GeometryWritable value = new GeometryWritable();

    GeometryRecordReader(GeometryCollection gc, int start, int end)
    {
      this.gc = gc;
      this.start = start;
      this.end = end;
      currentIndex = start - 1;
    }

    @Override
    public void close() throws IOException
    {
      if (gc != null)
      {
        gc.close();
      }
    }

    @Override
    public float getProgress() throws IOException
    {
      int size = end - start;
      return (float) (currentIndex - start + 1) / (float) size;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException
    {
      return key;
    }

    @Override
    public GeometryWritable getCurrentValue() throws IOException, InterruptedException
    {
      return value;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext arg1) throws IOException,
        InterruptedException
    {
      GeometryInputSplit gis = (GeometryInputSplit) split;

      this.gc = gis.gc;
      this.start = gis.startIndex;
      this.end = gis.endIndex;
      currentIndex = start - 1;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
      currentIndex++;
      if (currentIndex < end)
      {
        key.set(currentIndex);
        value.set(gc.get(currentIndex));
        return true;
      }
      
      return false;
    }
  }

  public final static String GEOMETRY_COLLECTION = "GeometryInputFormat.GeometryCollection";

  public static void setInput(Configuration conf, GeometryCollection gc) throws IOException
  {
    conf.set(GEOMETRY_COLLECTION, Base64Utils.encodeObject(gc));
  }

  public GeometryInputFormat()
  {
  }

  public static GeometryCollection loadGeometryCollection(Configuration conf) throws IOException
  {
    try
    {
      if (conf.get(GEOMETRY_COLLECTION) != null)
      {
        return (GeometryCollection) Base64Utils.decodeToObject(conf.get(GEOMETRY_COLLECTION));
      }
      else if (conf.get("mapred.input.dir") != null)
      {
        Path path = new Path(conf.get("mapred.input.dir"));
        if (path.toString().toLowerCase().endsWith(".shp"))
        {
          ShapefileReader sr = new ShapefileReader(path);
          
          // reproject into WGS84
          ReprojectedGeometryCollection rgc = new ReprojectedGeometryCollection(sr,
              WellKnownProjections.WGS84);

          return rgc;
        }
        else if (path.toString().toLowerCase().endsWith(".kml"))
        {
          return new KmlInputStream(path);
        }
      }
      throw new IllegalArgumentException("Neither a geometry collection or filename was set.");
    }
    catch (ClassNotFoundException e)
    {
      throw new IllegalArgumentException("Could not decode geometry collection", e);
    }

  }

  @Override
  public RecordReader<LongWritable, GeometryWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException
  {
    GeometryCollection gc = loadGeometryCollection(context.getConfiguration());

    GeometryInputSplit giSplit = (GeometryInputSplit) split;

    GeometryRecordReader reader = new GeometryRecordReader(gc, giSplit.getStart(), giSplit.getEnd());

    return reader;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    Configuration conf = context.getConfiguration();
    GeometryCollection gc = loadGeometryCollection(context.getConfiguration());
    int numSplits = conf.getInt("mapred.map.tasks", 2);

    // make sure there are at least 10k features per node.
    final int MIN_FEATURES_PER_SPLIT = 10000;
    if (gc.size() / MIN_FEATURES_PER_SPLIT < numSplits)
    {
      numSplits = (int) Math.ceil((double) gc.size() / (double)MIN_FEATURES_PER_SPLIT);
    }

    List<InputSplit> result = new LinkedList<InputSplit>();

    for (int i = 0; i < numSplits; i++)
    {
      int start = (int) Math.round((double) i * (double) gc.size() / numSplits);
      int end = (int) Math.round((double) (i + 1) * (double) gc.size() / numSplits);
      result.add(new GeometryInputSplit(gc, start, end));
    }

    return result;
  }
}
