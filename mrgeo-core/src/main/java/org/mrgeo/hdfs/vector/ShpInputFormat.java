/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.hdfs.vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.hdfs.vector.shp.ShapefileReader;
import org.mrgeo.utils.GDALUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class ShpInputFormat extends InputFormat<FeatureIdWritable, Geometry>
{
  static public class GeometryInputSplit extends InputSplit implements Writable
  {
    private static final long serialVersionUID = 1L;

    // End index is exclusive. I.e. if start = 0 and end = 5 only 0, 1, 2, 3 and 4 will be
    // processed.
    int endIndex;
    int startIndex;

    /**
     * This is here so that we can re-create the input split on the
     * remote side. It uses reflection to construct the instance and
     * the Writable.readFields method to populate it.
     */
    public GeometryInputSplit()
    {
    }

    public GeometryInputSplit(int start, int end)
    {
      startIndex = start;
      endIndex = end;
    }

    public int getEnd()
    {
      return endIndex;
    }

    @Override
    public long getLength() throws IOException
    {
      return (long)endIndex - startIndex;
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

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
      dataOutput.writeInt(startIndex);
      dataOutput.writeInt(endIndex);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
      startIndex = dataInput.readInt();
      endIndex = dataInput.readInt();
    }
  }

  static public class ShpRecordReader extends RecordReader<FeatureIdWritable, Geometry>
  {
    private int currentIndex;
    private int end;
    private ShapefileGeometryCollection gc;
    private int start;
    private FeatureIdWritable key = new FeatureIdWritable();
    private Geometry value = null;

    ShpRecordReader()
    {
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
    public FeatureIdWritable getCurrentKey() throws IOException, InterruptedException
    {
      return key;
    }

    @Override
    public Geometry getCurrentValue() throws IOException, InterruptedException
    {
      return value;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
        InterruptedException
    {
      if (split instanceof GeometryInputSplit)
      {
        gc = loadGeometryCollection(context.getConfiguration());

        GeometryInputSplit gis = (GeometryInputSplit) split;

        this.start = gis.startIndex;
        this.end = gis.endIndex;
        currentIndex = start - 1;
      }
      else
      {
        throw new IOException("input split is not a GeometryInputSplit");
      }

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
      currentIndex++;
      if (currentIndex < end)
      {
        key.set(currentIndex);
        value = gc.get(currentIndex);
        return true;
      }
        
      return false;
    }
  }

  public ShpInputFormat()
  {
  }

  private static ShapefileGeometryCollection loadGeometryCollection(Configuration conf) throws IOException
  {
    if (conf.get("mapred.input.dir") != null)
    {
      Path path = new Path(conf.get("mapred.input.dir"));
      if (path.toString().toLowerCase().endsWith(".shp"))
      {
        ShapefileReader sr = new ShapefileReader(path);

        // reproject into WGS84
        ReprojectedShapefileGeometryCollection rgc =
                new ReprojectedShapefileGeometryCollection(sr, GDALUtils.EPSG4326());

        return rgc;
      }
    }
    throw new IllegalArgumentException("Neither a geometry collection or filename was set.");
  }

  @Override
  public RecordReader<FeatureIdWritable, Geometry> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException
  {
    return new ShpRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    Configuration conf = context.getConfiguration();
    ShapefileGeometryCollection gc = loadGeometryCollection(context.getConfiguration());
    try
    {
      int numSplits = conf.getInt("mapred.map.tasks", 2);

      // make sure there are at least 10k features per node.
      final int MIN_FEATURES_PER_SPLIT = 10000;
      if (gc.size() / MIN_FEATURES_PER_SPLIT < numSplits)
      {
        numSplits = (int) Math.ceil((double) gc.size() / (double) MIN_FEATURES_PER_SPLIT);
      }

      List<InputSplit> result = new LinkedList<InputSplit>();

      for (int i = 0; i < numSplits; i++)
      {
        int start = (int) Math.round((double) i * (double) gc.size() / numSplits);
        int end = (int) Math.round((double) (i + 1) * (double) gc.size() / numSplits);
        result.add(new GeometryInputSplit(start, end));
      }

      return result;
    }
    finally
    {
        gc.close();
    }
  }
}
