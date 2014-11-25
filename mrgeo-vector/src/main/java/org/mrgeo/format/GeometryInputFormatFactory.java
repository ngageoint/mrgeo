/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.format;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.mrgeo.mapreduce.GeometryWritable;

import java.util.HashMap;

public class GeometryInputFormatFactory
{
  private static GeometryInputFormatFactory theInstance;
  private HashMap<String, Class<?>> formats;

  synchronized public static GeometryInputFormatFactory getInstance()
  {
    if (theInstance == null)
    {
      theInstance = new GeometryInputFormatFactory();
      theInstance.formats = new HashMap<String, Class<?>>();
      theInstance.formats.put(".shp", GeometryInputFormat.class);
      theInstance.formats.put(".kml", GeometryInputFormat.class);
      theInstance.formats.put(".tsv", TsvInputFormat.class);
      // Flickr format was deprecated
      //theInstance.formats.put(".fkr", FlickrInputFormat.class);
      theInstance.formats.put(".osm", OsmInputFormat.class);
      theInstance.formats.put(".seq", SequenceFileInputFormat.class);
    }
    return theInstance;
  }

  public InputFormat<LongWritable, GeometryWritable> createReader(Path path)
      throws IllegalArgumentException
  {
    InputFormat<LongWritable, GeometryWritable> result = null;
    String str = path.toString();
    str = str.substring(str.length() - 4, str.length());

    try
    {
      if (formats.containsKey(str))
      {
        result = (InputFormat<LongWritable, GeometryWritable>) formats.get(str).newInstance();
      }
    }
    catch (Exception e)
    {
      throw new IllegalArgumentException("Error instantiating input format");
    }
    
    if (result == null)
    {
      throw new IllegalArgumentException("File extension not supported.");
    }

    return result;
  }
}
