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

package org.mrgeo.hdfs.ingest.format;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.mrgeo.utils.LongRectangle;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IngestImageSplit extends InputSplit implements Writable
{
  private String filename;
  private int zoomlevel;
  private int tilesize;

  private long startTx;
  private long endTx;

  private long startTy;
  private long endTy;
  
  private LongRectangle imageBounds;
  
  
  
  private long totalTiles;

  private String location;

  public IngestImageSplit()
  {
    filename = null;
    
    zoomlevel = -1;
    tilesize = -1;

    startTx = 0;
    endTx = 0;

    startTy = 0;
    endTy = 0;
    
    imageBounds = new LongRectangle(0,0,0,0);

    location = null;
  }

  public IngestImageSplit(String filename, long startTx, long endTx, long startTy, long endTy, 
      long totalTiles, LongRectangle imageBounds, int zoomlevel, int tilesize, String location)
  {
    this.filename = filename;
    
    this.zoomlevel = zoomlevel;
    this.tilesize = tilesize; 
    this.startTx = startTx;
    this.endTx = endTx;
    this.startTy = startTy;
    this.endTy = endTy;
    this.totalTiles = totalTiles;

    this.imageBounds = imageBounds;
    this.location = location;
  }

  public long getStartTx()
  {
    return startTx;
  }
  public long getStartTy()
  {
    return startTy;
  }
  public long getEndTx()
  {
    return endTx;
  }
  public long getEndTy()
  {
    return endTy;
  }
  public long getTotalTiles()
  {
    return totalTiles;
  }
  
  public int getZoomlevel()
  {
    return zoomlevel;
  }
  public int getTilesize()
  {
    return tilesize;
  }

  public String getFilename()
  {
    return filename;
  }
  
  public LongRectangle getImageBounds()
  {
    return imageBounds;
  }
  
  @Override
  public long getLength() throws IOException, InterruptedException
  {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException
  {
    final String[] locations = { location };
    return locations;
  }

  @Override
  public void readFields(DataInput in) throws IOException
  {  
    filename = in.readUTF();
    if (filename.isEmpty())
    {
      filename = null;
    }
    
    zoomlevel = in.readInt();
    tilesize = in.readInt();
    startTx = in.readLong();
    endTx = in.readLong();
    startTy = in.readLong();
    endTy = in.readLong();
    totalTiles = in.readLong();

    long x1, x2, y1, y2;
    x1 = in.readLong();
    y1 = in.readLong();
    x2 = in.readLong();
    y2 = in.readLong();
    imageBounds = new LongRectangle(x1, y1, x2, y2);
    
    location = in.readUTF();
    if (location.isEmpty())
    {
      location = null;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeUTF(filename == null ? "" : filename);
    
    out.writeInt(zoomlevel);
    out.writeInt(tilesize);
    out.writeLong(startTx);
    out.writeLong(endTx);
    out.writeLong(startTy);
    out.writeLong(endTy);
    out.writeLong(totalTiles);

    out.writeLong(imageBounds.getMinX());
    out.writeLong(imageBounds.getMinY());
    out.writeLong(imageBounds.getMaxX());
    out.writeLong(imageBounds.getMaxY());
    
    out.writeUTF(location == null ? "" : location);
  }

}
