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
