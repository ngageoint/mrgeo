package org.mrgeo.data.geowave.vector;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.KVIterator;
import org.mrgeo.data.vector.VectorDataProvider;
import org.mrgeo.data.vector.VectorMetadata;
import org.mrgeo.data.vector.VectorMetadataReader;
import org.mrgeo.data.vector.VectorReader;
import org.mrgeo.geometry.Geometry;

public class GeoWaveTester
{
  public static void main(String[] args)
  {
    GeoWaveTester tester = new GeoWaveTester();
    try
    {
      tester.runTest();
    }
    catch (IOException e)
    {
      System.err.println("Exception while running test: " + e);
//      e.printStackTrace();
    }
  }

  public void runTest() throws IOException
  {
    VectorDataProvider vdp = DataProviderFactory.getVectorDataProvider("geowave:Af_Clip", AccessMode.READ);
    VectorMetadataReader reader = vdp.getMetadataReader();
    VectorMetadata metadata = reader.read();
    for (String attr : metadata.getAttributes())
    {
      System.out.println("attribute: " + attr);
    }
    VectorReader vr = vdp.getVectorReader();
    System.out.println("Values:");
    KVIterator<LongWritable,Geometry> iter = vr.get();
    LongWritable lastId = null;
    while (iter.hasNext())
    {
      Geometry geom = iter.next();
      LongWritable key = iter.currentKey();
      lastId = key;
      System.out.println("key: " + key.toString());
      TreeMap<String,String> attrs = geom.getAllAttributesSorted();
      for (java.util.Map.Entry<String,String> entry: attrs.entrySet())
      {
        System.out.println("  " + entry.getKey() + " = " + entry.getValue());
      }
    }
    if (lastId != null)
    {
      System.out.println("Getting a specific key: " + lastId);
      if (vr.exists(lastId))
      {
        Geometry geom = vr.get(lastId);
        TreeMap<String,String> attrs = geom.getAllAttributesSorted();
        for (java.util.Map.Entry<String,String> entry: attrs.entrySet())
        {
          System.out.println("  " + entry.getKey() + " = " + entry.getValue());
        }
      }
      else
      {
        System.out.println("Feature does not exist... but it should");
      }
    }
    else
    {
      System.out.println("Cannot query for a single feature");
    }
  }
}
