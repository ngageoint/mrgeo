/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.vector.mrsvector.MrsVectorPyramid;

import java.io.IOException;
import java.util.HashMap;

public class FeatureInputFormatFactory
{
  private static FeatureInputFormatFactory theInstance;
  private HashMap<String, Class<?>> formats;

  synchronized public static FeatureInputFormatFactory getInstance()
  {
    if (theInstance == null)
    {
      theInstance = new FeatureInputFormatFactory();
      theInstance.formats = new HashMap<String, Class<?>>();
      theInstance.formats.put(".csv", CsvInputFormat.class);
      theInstance.formats.put(".tsv", TsvInputFormat.class);
      theInstance.formats.put(".shp", ShpInputFormat.class);
      theInstance.formats.put(".sql", PgQueryInputFormat.class);
    }
    return theInstance;
  }

  public boolean isRecognized(Path path)
  {
    boolean result = false;
    // First, see if it's a MrsVectorPyramid
    MrsVectorPyramid pyramid = null;
    try
    {
      pyramid = MrsVectorPyramid.open(path.toString());
      result = true;
    }
    catch(IOException e)
    {
      // Ignore. The inability to load the pyramid is handled below.
    }
    // If it's not a vector pyramid, see if it's one of the supported
    // vector file formats.
    if (pyramid == null)
    {
      String str = path.toString();
      if (str.length() >= 4)
      {
        str = str.substring(str.length() - 4, str.length());
        if (formats.containsKey(str))
        {
          result = true;
        }
      }
    }

    return result;
  }

  public Class<?> getInputFormatClass(Path path)
  {
    String str = path.toString();
    str = str.substring(str.length() - 4, str.length());

    if (formats.containsKey(str))
    {
      return formats.get(str);
    }
    
    return null;

  }
  public InputFormat<LongWritable, Geometry> createInputFormat(String path) throws IllegalArgumentException
  {
    InputFormat<LongWritable, Geometry> result = null;
    String str = path;
    str = str.substring(str.length() - 4, str.length());
    try
    {
      if (formats.containsKey(str))
      {
        result = (InputFormat<LongWritable, Geometry>)formats.get(str).newInstance();
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
