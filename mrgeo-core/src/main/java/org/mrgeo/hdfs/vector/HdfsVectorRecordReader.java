package org.mrgeo.hdfs.vector;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.mrgeo.data.vector.VectorInputSplit;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.hdfs.utils.HadoopFileUtils;

public class HdfsVectorRecordReader extends RecordReader<LongWritable, Geometry>
{
  private DelimitedParser delimitedParser;
  private LineRecordReader recordReader;

  public class VectorLineProducer implements LineProducer
  {
    LineRecordReader lineRecordReader;
    public VectorLineProducer(LineRecordReader recordReader)
    {
      this.lineRecordReader = recordReader;
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public String nextLine() throws IOException
    {
      if (lineRecordReader.nextKeyValue())
      {
        return lineRecordReader.getCurrentValue().toString();
      }
      return null;
    }
  }

  public HdfsVectorRecordReader()
  {
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException
  {
    VectorInputSplit vis = (VectorInputSplit)split;
    FileSplit fsplit = (FileSplit)vis.getWrappedInputSplit();
    delimitedParser = getDelimitedParser(fsplit.getPath().toString(),
        context.getConfiguration());
    recordReader = new LineRecordReader();
    recordReader.initialize(fsplit, context);
    // Skip the first 
    if (delimitedParser.getSkipFirstLine())
    {
      // Only skip the first line of the first split. The other
      // splits are somewhere in the middle of the original file,
      // so their first lines should not be skipped.
      if (fsplit.getStart() != 0)
      {
        nextKeyValue();
      }
    }
  }

  public static DelimitedParser getDelimitedParser(String input, Configuration conf) throws IOException
  {
    char delimiter = ',';
    if (input.toLowerCase().endsWith(".tsv"))
    {
      delimiter = '\t';
    }
    Path columnsPath = new Path(input + ".columns");
    List<String> attributeNames = new ArrayList<String>();
    int xCol = -1;
    int yCol = -1;
    int geometryCol = -1;
    boolean skipFirstLine = false;
    FileSystem fs = HadoopFileUtils.getFileSystem(conf, columnsPath);
    if (fs.exists(columnsPath))
    {
      InputStream in = null;
      try
      {
        in = HadoopFileUtils.open(conf, columnsPath); // fs.open(columnPath);
        ColumnDefinitionFile cdf = new ColumnDefinitionFile(in);
        skipFirstLine = cdf.isFirstLineHeader();

        int i = 0;
        for (Column col : cdf.getColumns())
        {
          String c = col.getName();

          if (col.getType() == Column.FactorType.Numeric)
          {
            if (c.equals("x"))
            {
              xCol = i;
            }
            else if (c.equals("y"))
            {
              yCol = i;
            }
          }
          else
          {
            if (c.toLowerCase().equals("geometry"))
            {
              geometryCol = i;
            }
          }

          attributeNames.add(c);
          i++;
        }
      }
      finally
      {
        if (in != null)
        {
          in.close();
        }
      }
    }
    else
    {
      throw new IOException("Column file was not found.");
    }
    DelimitedParser delimitedParser = new DelimitedParser(attributeNames,
        xCol, yCol, geometryCol, delimiter, '\"', skipFirstLine);
    return delimitedParser;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    return recordReader.nextKeyValue();
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException
  {
    return recordReader.getCurrentKey();
  }

  @Override
  public Geometry getCurrentValue() throws IOException, InterruptedException
  {
    Text rawValue = recordReader.getCurrentValue();
    if (rawValue == null)
    {
      return null;
    }
    return delimitedParser.parse(rawValue.toString());
  }

  @Override
  public float getProgress() throws IOException, InterruptedException
  {
    return recordReader.getProgress();
  }

  @Override
  public void close() throws IOException
  {
  }
}
