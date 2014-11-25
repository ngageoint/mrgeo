package org.mrgeo.pig;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.mrgeo.column.ColumnDefinitionFile;
import org.mrgeo.format.CsvOutputFormat;
import org.mrgeo.format.CsvOutputFormat.CsvRecordWriter;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.WritableGeometry;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class CsvStoreFunc extends StoreFunc implements StoreMetadata
{
  private static final Logger log = LoggerFactory.getLogger(CsvStoreFunc.class);

  CsvRecordWriter _writer;
  WritableGeometry _feature;
  LongWritable _zero = new LongWritable(0);

  @Override
  public OutputFormat<LongWritable, Geometry> getOutputFormat() throws IOException
  {
    return new CsvOutputFormat();
  }

  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException
  {
    _writer = (CsvRecordWriter) writer;
    _writer.setDelimiter('\t');
  }


  @Override
  public void putNext(Tuple t) throws IOException
  {
    _feature = GeometryFactory.createEmptyGeometry();

    int i = 0;
    for (Object o : t.getAll())
    {
        _feature.setAttribute(Integer.toString(i), DataType.toString(o));
      i++;
    }

    _writer.write(_zero, _feature);
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException
  {
    FileOutputFormat.setOutputPath(job, new Path(location));
  }

  @Override
  public void storeSchema(ResourceSchema pigSchema, String location, Job job) throws IOException
  {
    log.warn("storeSchema()");


    ArrayList<String> names = new ArrayList<>();
    for (ResourceFieldSchema field : pigSchema.getFields())
    {
      names.add(field.getName());
    }

    Collections.sort(names);


    ColumnDefinitionFile cdf = new ColumnDefinitionFile();
    cdf.setColumns(names);
    cdf.setFirstLineHeader(false);
    Path p = new Path(location + ".columns");
    FileSystem fs = HadoopFileUtils.getFileSystem(p);
    fs.delete(p, false);
    FSDataOutputStream os = fs.create(p);
    cdf.store(os);
    cdf.store(System.out);
    os.flush();
    os.close();
  }

  @Override
  public void storeStatistics(ResourceStatistics arg0, String arg1, Job arg2) throws IOException
  {
    log.warn("storeStatistics()");
    // not needed.
  }
}
