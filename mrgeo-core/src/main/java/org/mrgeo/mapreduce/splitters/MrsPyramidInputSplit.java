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

package org.mrgeo.mapreduce.splitters;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.HadoopUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class is an InputSplit used by the MrGeo core for MrsPyramid data. It
 * wraps an actual "native" split provided by a data plugin and includes
 * additional information required by the MrGeo core. The native split is
 * created by the data plugin provider so that pyramids can be split in
 * whatever way is appropriate for the storage format. For example, a FileSplit
 * would be an appropriate native split format for a file-based data store, but
 * something else would likely be used if the data store were a database. 
 */
public class MrsPyramidInputSplit extends InputSplit implements Writable
{
  private TiledInputSplit wrappedInputSplit;
  private String name;
  private Bounds[] preBounds;
  private Bounds[] postBounds;
  private int zoomlevel;

  public MrsPyramidInputSplit()
  {
  }

  public MrsPyramidInputSplit(final TiledInputSplit split, String name, int zoomlevel,
      Bounds[] preBounds, Bounds[] postBounds) throws IOException
  {
    super();
    this.wrappedInputSplit = split;
    this.name = name;
    this.preBounds = preBounds;
    this.postBounds = postBounds;
    this.zoomlevel = zoomlevel;
  }

  public String getName()
  {
    return name;
  }

  public Bounds[] getPreBounds()
  {
    return preBounds;
  }
  
  public Bounds[] getPostBounds()
  {
    return postBounds;
  }

  public int getZoomlevel()
  {
    return zoomlevel;
  }

  @Override
  public void readFields(DataInput in) throws IOException
  {
    boolean wrappedWritable = in.readBoolean();
    if (wrappedWritable)
    {
      String wrappedSplitClassName = in.readUTF();
      try
      {
        Class<?> splitClass = Class.forName(wrappedSplitClassName);
        wrappedInputSplit = (TiledInputSplit)ReflectionUtils.newInstance(splitClass, HadoopUtils.createConfiguration());
        ((Writable)wrappedInputSplit).readFields(in);
      }
      catch (ClassNotFoundException e)
      {
        throw new IOException(e);
      }
    }

    name = in.readUTF();
    zoomlevel = in.readInt();
    int presize = in.readInt();
    preBounds = new Bounds[presize];
    for (int i = 0; i < presize; i++)
    {
      preBounds[i] = new Bounds(in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble());
    }

    int postsize = in.readInt();
    postBounds = new Bounds[postsize];
    for (int i = 0; i < postsize; i++)
    {
      postBounds[i] = new Bounds(in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble());
    }
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    // Write a boolean indicating whether the wrapped input split is writable. If
    // it is, then write it after the boolean.
    if (wrappedInputSplit instanceof Writable)
    {
      out.writeBoolean(true);
      out.writeUTF(wrappedInputSplit.getClass().getName());
      ((Writable)wrappedInputSplit).write(out);
    }
    else
    {
      out.writeBoolean(false);
    }

    out.writeUTF(name);
    out.writeInt(zoomlevel);

    out.writeInt(preBounds.length);
    for (int i = 0; i < preBounds.length; i++)
    {
      out.writeDouble(preBounds[i].getMinX());
      out.writeDouble(preBounds[i].getMinY());
      out.writeDouble(preBounds[i].getMaxX());
      out.writeDouble(preBounds[i].getMaxY());
    }

    out.writeInt(postBounds.length);
    for (int i = 0; i < postBounds.length; i++)
    {
      out.writeDouble(postBounds[i].getMinX());
      out.writeDouble(postBounds[i].getMinY());
      out.writeDouble(postBounds[i].getMaxX());
      out.writeDouble(postBounds[i].getMaxY());
    }
  }

  @Override
  public long getLength() throws IOException, InterruptedException
  {
    return wrappedInputSplit.getLength();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException
  {
    return wrappedInputSplit.getLocations();
  }

  public TiledInputSplit getWrappedSplit()
  {
    return wrappedInputSplit;
  }
}
