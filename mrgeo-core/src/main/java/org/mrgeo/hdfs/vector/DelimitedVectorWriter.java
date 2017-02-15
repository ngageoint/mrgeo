/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.hdfs.vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.data.vector.VectorWriter;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.WktConverter;
import org.mrgeo.hdfs.utils.HadoopFileUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.TreeMap;

public class DelimitedVectorWriter implements VectorWriter
{
private HdfsVectorDataProvider provider;
private PrintWriter out;
private Configuration conf;
private String[] attributeNames;
private String delimiter = ",";
// The string to use for encapsulating field values. This allows fields to
// contain embedded delimiter characters.
private String encapsulator = "\"";

public DelimitedVectorWriter(HdfsVectorDataProvider provider, Configuration conf)
{
  this.provider = provider;
  this.conf = conf;
}

@Override
public void append(FeatureIdWritable key, Geometry value) throws IOException
{
  if (out == null)
  {
    String resolvedName = provider.getResolvedResourceName(false);
    Path outputPath = new Path(resolvedName);
    if (resolvedName.toLowerCase().endsWith(".tsv"))
    {
      delimiter = "\t";
    }

    FileSystem fs = HadoopFileUtils.getFileSystem(conf, outputPath);
    OutputStream os = fs.create(outputPath);
    out = new PrintWriter(os);
    TreeMap<String, String> sortedAttributes = value.getAllAttributesSorted();
    attributeNames = new String[sortedAttributes.size()];
    // Store a list of attribute names so that we can write them in the
    // same order for every feature. At the same time, we build up the
    // attribute header.
    StringBuffer header = new StringBuffer();
    header.append("GEOMETRY");
    int i = 0;
    for (String attributeName : sortedAttributes.navigableKeySet())
    {
      attributeNames[i] = attributeName;
      if (header.length() > 0)
      {
        header.append(delimiter);
      }
      boolean containsDelimiter = attributeName.contains(delimiter);
      if (containsDelimiter)
      {
        header.append(encapsulator);
      }
      header.append(attributeName);
      if (containsDelimiter)
      {
        header.append(encapsulator);
      }
      i++;
    }
    // Write the header line which is a delimited list of attribute names
    out.println(header.toString());
  }

  StringBuffer strFeature = new StringBuffer();
  strFeature.append(WktConverter.toWkt(value));
  for (String attributeName : attributeNames)
  {
    if (strFeature.length() > 0)
    {
      strFeature.append(delimiter);
    }
    String attrValue = value.getAttribute(attributeName);
    if (attrValue != null)
    {
      boolean containsDelimiter = attrValue.contains(delimiter);
      if (containsDelimiter)
      {
        strFeature.append(encapsulator);
      }
      strFeature.append(attrValue);
      if (containsDelimiter)
      {
        strFeature.append(encapsulator);
      }
    }
  }
  out.println(strFeature.toString());
}

@Override
public void close() throws IOException
{
  if (out != null)
  {
    out.close();
  }
}
}
