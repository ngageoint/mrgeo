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

package org.mrgeo.hdfs.input.image;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.*;

public class SimplePyramidInputSplit extends InputSplit implements Writable, Serializable
{
  public String pyramid;
  public long startId;
  public long endId;
  public int zoom;
  public int tilesize;

  public SimplePyramidInputSplit()
  {

  }

  public SimplePyramidInputSplit(String pyramid, long startTileId,
      long endTileId, int zoom, int tilesize)
  {
    this.startId = startTileId;
    this.endId = endTileId;
    this.pyramid = pyramid;
    this.tilesize = tilesize;
    this.zoom = zoom;
  }

  private synchronized void writeObject(ObjectOutputStream out) throws IOException
  {
    out.writeUTF(pyramid);
    out.writeLong(startId);
    out.writeLong(endId);
    out.writeInt(zoom);
    out.writeInt(tilesize);
  }

  private synchronized void readObject(ObjectInputStream in) throws IOException
  {
    pyramid = in.readUTF();
    startId = in.readLong();
    endId = in.readLong();
    zoom = in.readInt();
    tilesize = in.readInt();
  }

  @Override
  public long getLength() throws IOException, InterruptedException
  {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException
  {
    return new String[0];
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeUTF(pyramid);
    out.writeLong(startId);
    out.writeLong(endId);
    out.writeInt(zoom);
    out.writeInt(tilesize);
  }

  @Override
  public void readFields(DataInput in) throws IOException
  {
    pyramid = in.readUTF();
    startId = in.readLong();
    endId = in.readLong();
    zoom = in.readInt();
    tilesize = in.readInt();
  }
}
