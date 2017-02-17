/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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
import org.mrgeo.utils.HadoopUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TiledInputSplit extends InputSplit implements Writable
{
private InputSplit wrappedInputSplit;
private long startTileId;
private long endTileId;
private int zoomLevel;
private int tileSize;

// Used by Hadoop to re-construct the split when readFields is called.
public TiledInputSplit()
{
}

public TiledInputSplit(final InputSplit inputSplit, long startTileId,
    long endTileId, int zoomLevel, int tileSize)
{
  this.wrappedInputSplit = inputSplit;
  this.startTileId = startTileId;
  this.endTileId = endTileId;
  this.zoomLevel = zoomLevel;
  this.tileSize = tileSize;
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
      wrappedInputSplit = (InputSplit) ReflectionUtils.newInstance(splitClass, HadoopUtils.createConfiguration());
      ((Writable) wrappedInputSplit).readFields(in);
    }
    catch (ClassNotFoundException e)
    {
      throw new IOException(e);
    }
  }

  startTileId = in.readLong();
  endTileId = in.readLong();
  zoomLevel = in.readInt();
  tileSize = in.readInt();
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
    ((Writable) wrappedInputSplit).write(out);
  }
  else
  {
    out.writeBoolean(false);
  }
  out.writeLong(startTileId);
  out.writeLong(endTileId);
  out.writeInt(zoomLevel);
  out.writeInt(tileSize);
}

@Override
public long getLength() throws IOException, InterruptedException
{
  return (wrappedInputSplit != null) ? wrappedInputSplit.getLength() : 0;
}

@Override
public String[] getLocations() throws IOException, InterruptedException
{
  return (wrappedInputSplit != null) ? wrappedInputSplit.getLocations() : new String[0];
}

public InputSplit getWrappedSplit()
{
  return wrappedInputSplit;
}

public long getStartTileId()
{
  return startTileId;
}

public long getEndTileId()
{
  return endTileId;
}

public int getZoomLevel()
{
  return zoomLevel;
}

public int getTileSize()
{
  return tileSize;
}
}
