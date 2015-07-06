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

package org.mrgeo.vector.formats;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.mrgeo.mapreduce.readers.HdfsMrsVectorPyramidRecordReader;
import org.mrgeo.vector.mrsvector.VectorTileWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.input.MapFileFilter;

import java.io.IOException;
import java.util.Set;

public class HdfsMrsVectorPyramidInputFormat extends SequenceFileInputFormat<TileIdWritable,VectorTileWritable>
{
  @Override
  public RecordReader<TileIdWritable, VectorTileWritable> createRecordReader(final InputSplit split,
      final TaskAttemptContext context)
      throws IOException
  {
    
    return new HdfsMrsVectorPyramidRecordReader();
  }

  public static void setInputInfo(final Job job, final int zoomlevel, final Set<String> inputs) throws IOException
  {
    job.setInputFormatClass(HdfsMrsVectorPyramidInputFormat.class);
    
    //final String scannedInput = inputs.get(0);
    //FileInputFormat.addInputPath(job, new Path(scannedInput));

    for (String input: inputs)
    {
      FileInputFormat.addInputPath(job, new Path(input));
    }
    FileInputFormat.setInputPathFilter(job, MapFileFilter.class);
  }
}
