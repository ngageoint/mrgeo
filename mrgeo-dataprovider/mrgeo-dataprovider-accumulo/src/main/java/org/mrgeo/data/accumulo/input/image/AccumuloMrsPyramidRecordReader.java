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

package org.mrgeo.data.accumulo.input.image;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;

import java.io.IOException;

public class AccumuloMrsPyramidRecordReader extends RecordReader<TileIdWritable, RasterWritable>
{

@Override
public void close() throws IOException
{
  // TODO Auto-generated method stub

}

@Override
public TileIdWritable getCurrentKey() throws IOException, InterruptedException
{
  // TODO Auto-generated method stub
  return null;
}

@Override
public RasterWritable getCurrentValue() throws IOException, InterruptedException
{
  // TODO Auto-generated method stub
  return null;
}

@Override
public float getProgress() throws IOException, InterruptedException
{
  // TODO Auto-generated method stub
  return 0;
}

@Override
public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException,
    InterruptedException
{
  // TODO Auto-generated method stub

}

@Override
public boolean nextKeyValue() throws IOException, InterruptedException
{
  // TODO Auto-generated method stub
  return false;
}


} // end AccumuloMrsPyramidRecordReader
