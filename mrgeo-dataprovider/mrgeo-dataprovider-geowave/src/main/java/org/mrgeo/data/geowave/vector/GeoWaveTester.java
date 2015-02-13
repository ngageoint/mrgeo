package org.mrgeo.data.geowave.vector;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorInputFormat;
import org.mrgeo.data.vector.VectorInputFormatContext;
import org.mrgeo.data.vector.VectorInputFormatProvider;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.utils.HadoopUtils;

public class GeoWaveTester
{
  public static void main(String[] args)
  {
    if (args.length != 2 && args.length != 3)
    {
      System.err.println("GeoWaveTester <input> <output>");
      return;
    }
    GeoWaveTester tester = new GeoWaveTester();
    try
    {
      String input = args[0];
      String output = args[1];
      String userRoles = null;
      if (args.length == 3)
      {
        userRoles = args[2];
      }
      tester.runTest(input, output, userRoles);
    }
    catch (IOException e)
    {
      System.err.println("Exception while running test: " + e);
//      e.printStackTrace();
    }
  }

  public static class TestMapper extends Mapper<LongWritable, Geometry, Text, Text>
  {
    @Override
    protected void map(LongWritable key, Geometry value, Context context) throws IOException,
        InterruptedException
    {
//      System.out.println("Key: " + key.get());
//      System.out.println("Value: " + value);
      context.write(new Text("" + key.get()), new Text(value.toString()));
    }
  }

  public static class TestDriver
  {
    public TestDriver()
    {
    }

    public void run(String input, String output, String userRoles)
    {
      try
      {
        Job job = new Job(HadoopUtils.createConfiguration());
//        HadoopUtils.setupLocalRunner(job.getConfiguration());
        HadoopUtils.setJar(job, TestDriver.class);
        job.setMapperClass(TestMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(VectorInputFormat.class);
        Properties providerProperties = new Properties();
        if (userRoles != null)
        {
          providerProperties.setProperty(DataProviderFactory.PROVIDER_PROPERTY_USER_ROLES, userRoles);
        }
        VectorDataProvider vpd = DataProviderFactory.getVectorDataProvider(input, AccessMode.READ, providerProperties);
        Set<String> inputs = new HashSet<String>();
        inputs.add(input);
        VectorInputFormatContext ifc = new VectorInputFormatContext(inputs,
            providerProperties);
        VectorInputFormatProvider ifp = vpd.getVectorInputFormatProvider(ifc);
        ifp.setupJob(job, providerProperties);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        final FileSystem fs = FileSystem.get(job.getConfiguration());
        final Path outputPath = new Path(output);
        fs.delete(
            outputPath,
            true);
        FileOutputFormat.setOutputPath(
            job,
            outputPath);
  
        final boolean jobSuccess = job.waitForCompletion(true);
        System.out.println("jobSuccess: " + jobSuccess);
      }
      catch(IOException e)
      {
        System.out.println("Got IOException: " + e);
      }
      catch (ClassNotFoundException e)
      {
        System.out.println("Got ClassNotFoundException: " + e);
      }
      catch (InterruptedException e)
      {
        System.out.println("Got InterruptedException: " + e);
      }
    }
  }

  public void runTest(String input, String output, String userRoles) throws IOException
  {
//    VectorDataProvider vdp = DataProviderFactory.getVectorDataProvider("geowave:Af_Clip", AccessMode.READ);
//    VectorMetadataReader reader = vdp.getMetadataReader();
//    VectorMetadata metadata = reader.read();
//    for (String attr : metadata.getAttributes())
//    {
//      System.out.println("attribute: " + attr);
//    }
//    VectorReader vr = vdp.getVectorReader();
//    System.out.println("Values:");
//    KVIterator<LongWritable,Geometry> iter = vr.get();
//    LongWritable lastId = null;
//    while (iter.hasNext())
//    {
//      Geometry geom = iter.next();
//      LongWritable key = iter.currentKey();
//      lastId = key;
//      System.out.println("key: " + key.toString());
//      TreeMap<String,String> attrs = geom.getAllAttributesSorted();
//      for (java.util.Map.Entry<String,String> entry: attrs.entrySet())
//      {
//        System.out.println("  " + entry.getKey() + " = " + entry.getValue());
//      }
//    }
//    if (lastId != null)
//    {
//      System.out.println("Getting a specific key: " + lastId);
//      if (vr.exists(lastId))
//      {
//        Geometry geom = vr.get(lastId);
//        TreeMap<String,String> attrs = geom.getAllAttributesSorted();
//        for (java.util.Map.Entry<String,String> entry: attrs.entrySet())
//        {
//          System.out.println("  " + entry.getKey() + " = " + entry.getValue());
//        }
//      }
//      else
//      {
//        System.out.println("Feature does not exist... but it should");
//      }
//    }
//    else
//    {
//      System.out.println("Cannot query for a single feature");
//    }
    
    TestDriver driver = new TestDriver();
//    driver.run("geowave:Af_Clip", "/user/dave.johnson/out");
    driver.run(input, output, userRoles);
  }
}
