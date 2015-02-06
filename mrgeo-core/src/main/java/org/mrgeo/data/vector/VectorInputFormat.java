package org.mrgeo.data.vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.geometry.Geometry;

public class VectorInputFormat extends InputFormat<LongWritable, Geometry>
{
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    VectorInputFormatContext ifContext = VectorInputFormatContext.load(context.getConfiguration());
    List<InputSplit> results = new ArrayList<InputSplit>();
    for (String input: ifContext.getInputs())
    {
      List<InputSplit> nativeSplits = getNativeSplits(context, ifContext, input);
      if (nativeSplits != null && !nativeSplits.isEmpty())
      {
        for (InputSplit nativeSplit: nativeSplits)
        {
          VectorInputSplit newSplit = new VectorInputSplit(input, nativeSplit);
          results.add(newSplit);
        }
      }
    }
    return results;
  }

  @Override
  public RecordReader<LongWritable, Geometry> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException
  {
    if (split instanceof VectorInputSplit)
    {
      throw new IOException("Expected a VectorInputSplit but got " + split.getClass().getName());
    }
    VectorInputSplit inputSplit = (VectorInputSplit)split;
    VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(context.getConfiguration(),
        inputSplit.getVectorName(), AccessMode.READ);
    RecordReader<LongWritable, Geometry> recordReader = dp.getRecordReader();
    recordReader.initialize(inputSplit.getWrappedInputSplit(), context);
    return recordReader;
  }
  
  private List<InputSplit> getNativeSplits(JobContext context,
      VectorInputFormatContext ifContext,
      String input) throws IOException, InterruptedException
  {
    VectorDataProvider dp = DataProviderFactory.getVectorDataProvider(context.getConfiguration(),
        input, AccessMode.READ);
    VectorInputFormatProvider ifProvider = dp.getVectorInputFormatProvider(ifContext);
    return ifProvider.getInputFormat(input).getSplits(context);
  }
}
