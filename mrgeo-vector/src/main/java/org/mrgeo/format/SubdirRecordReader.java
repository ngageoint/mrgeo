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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class SubdirRecordReader extends RecordReader<Text, Text>
{
  private Text subdir;
  private Text value;
  boolean more = false;

  @Override
  public void close() throws IOException
  {
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException
  {
    return subdir;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException
  {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException
  {
    // It's either all or none since there is only one record
    return (more) ? 0 : 1;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException
  {
    DirectorySplit ds = (DirectorySplit) split;
    subdir = new Text(ds.getPath().toString());
    value = new Text(ds.getPath().toString());
    more = true;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    // Only one K/V pair can be returned - it's the only "record" in the split
    if (!more)
    {
      subdir = null;
      value = null;
      return false;
    }
    more = false;
    return true;
  }

}
