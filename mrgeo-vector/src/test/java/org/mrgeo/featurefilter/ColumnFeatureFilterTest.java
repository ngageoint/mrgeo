package org.mrgeo.featurefilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.mrgeo.format.CsvInputFormat;
import org.mrgeo.format.TsvInputFormat;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class ColumnFeatureFilterTest
{
  protected static CsvInputFormat.CsvRecordReader getRecordReader(String filename) throws IOException
  {
    String inputDir = TestUtils.composeInputDir(ColumnFeatureFilterTest.class);
    Job j = new Job(new Configuration());
    Configuration c = j.getConfiguration();
    FileSystem fs = new RawLocalFileSystem();
    fs.setConf(c);
    Path testFile = new Path(inputDir, filename);
    testFile = fs.makeQualified(testFile);
    FileInputFormat.addInputPath(j, testFile);
    FileSplit split = new FileSplit(testFile, 0, 1000000000, null);
    
    TsvInputFormat.TsvRecordReader reader = new TsvInputFormat.TsvRecordReader();
    reader.initialize(split, HadoopUtils.createTaskAttemptContext(c, new TaskAttemptID()));
    
    fs.close();
    
    return reader;
  }
  
  protected static List<Geometry> readFeatures(String fileName) throws IOException,
    InterruptedException
  {
    CsvInputFormat.CsvRecordReader reader = getRecordReader(fileName);
    List<Geometry> features = new ArrayList<>();
    while (reader.nextKeyValue())
    {
      features.add(reader.getCurrentValue().createWritableClone());
    }
    reader.close();
    return features;
  } 
}
