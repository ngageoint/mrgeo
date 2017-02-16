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

import com.vividsolutions.jts.io.WKTReader;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is responsible for converting a line of text into a Geometry
 * object. It is configured with the following values:
 * <ul>
 * <li> the column numbers that contain the x/y coordinates (for point data,
 * if any) </li>
 * <li>the column number that contains the geometry of the feature in WKT format
 * (if any)</li>
 * <li>the list of attribute names for all of the columns in the data source</li>
 * <li>the delimiter character that separates individual fields</li>
 * <li>the character that is used to encapsulate strings (e.g. double quote)</li>
 * <li>a flag indicating whether the first line of data should be skipped - useful
 * for cases where the first line is the column header</li>
 * </ul>
 */
public class DelimitedParser implements Externalizable
{
static final Logger log = LoggerFactory.getLogger(DelimitedParser.class);

private List<String> attributeNames;
private int xCol;
private int yCol;
private int geometryCol;
private char delimiter;
private char encapsulator;
private boolean skipFirstLine;
private WKTReader _wktReader;

/**
 * Should only be used for serialization.
 */
public DelimitedParser()
{
}

public DelimitedParser(List<String> attributeNames, int xCol, int yCol,
    int geometryCol, char delimiter, char encapsulator, boolean skipFirstLine)
{
  this.attributeNames = attributeNames;
  this.xCol = xCol;
  this.yCol = yCol;
  this.delimiter = delimiter;
  this.encapsulator = encapsulator;
  this.geometryCol = geometryCol;
  this.skipFirstLine = skipFirstLine;
}

static String[] split(String line, char delimiter, char encapsulator)
{
  ArrayList<String> result = new ArrayList<String>();

  StringBuffer buf = new StringBuffer();

  for (int i = 0; i < line.length(); i++)
  {
    char c = line.charAt(i);
    if (c == delimiter)
    {
      result.add(buf.toString());
      buf.delete(0, buf.length());
    }
    else if (c == encapsulator)
    {
      // skip the first encapsulator
      i++;
      // clear out the buffer
      buf.delete(0, buf.length());
      // add data until we hit another encapsulator
      while (i < line.length() && line.charAt(i) != encapsulator)
      {
        c = line.charAt(i++);
        buf.append(c);
      }

      // add the encapsulated string
      result.add(buf.toString());
      // clear out the buffer
      buf.delete(0, buf.length());
      // skip the last encapsulator
      i++;

      if (i >= line.length())
      {
//          log.error("Missing token end character (" + encapsulator +
//              ") in line: " + line);

        // need to return here, or we will add a blank field on the end of the result
        return result.toArray(new String[result.size()]);
      }

      // find the next delimiter. There may be white space or something between.
      while (i < line.length() && line.charAt(i) != delimiter)
      {
        i++;
      }
    }
    else
    {
      buf.append(c);
    }
  }

  result.add(buf.toString());

  return result.toArray(new String[result.size()]);
}

public char getDelimiter()
{
  return delimiter;
}

public boolean getSkipFirstLine()
{
  return skipFirstLine;
}

@SuppressWarnings("squid:S1166") // Exception caught and handled
public Geometry parse(String line)
{
  if (_wktReader == null)
  {
    _wktReader = new WKTReader();
  }

  Geometry feature = null;

  Double x = null, y = null;
  String wktGeometry = null;
  Map<String, String> attrs = new HashMap<>();

  // if there is only 1 column, we don't need to split
  String[] values;
  if (attributeNames.size() == 1)
  {
    // use the whole line, but still take out the encapsulator
    values = split(line, '\n', encapsulator);
  }
  else
  {
    values = split(line, delimiter, encapsulator);
  }
  if (values.length == 0)
  {
    log.info("Values empty. Weird.");
  }

  if (geometryCol < 0 && xCol < 0 && yCol < 0)
  {
    for (int i = 0; i < values.length; i++)
    {
      if (WktGeometryUtils.isValidWktGeometry(values[i]))
      {
        attributeNames = new ArrayList<>(values.length);
        for (int j = 0; j < values.length; j++)
        {
          if (j == i)
          {
            geometryCol = i;
          }

          attributeNames.add(Integer.toString(i));
        }
        break;
      }
    }
  }

  for (int i = 0; i < values.length; i++)
  {
    if (i == geometryCol)
    {
      wktGeometry = values[i];
    }
    else if (i == xCol)// && values[i] != null && values[i].length() > 0)
    {
      try
      {
        if (values[i].trim().length() > 0)
        {
          x = Double.parseDouble(values[i]);
        }
        else
        {
          x = null;
        }
      }
      catch (NumberFormatException e)
      {
        log.error("Invalid numeric value for x: " + values[i] + ". Continuing with null x value.");
        x = null;
      }
    }
    else if (i == yCol)// && values[i] != null && values[i].length() > 0)
    {
      try
      {
        if (values[i].trim().length() > 0)
        {
          y = Double.parseDouble(values[i]);
        }
        else
        {
          y = null;
        }
      }
      catch (NumberFormatException e)
      {
        log.error("Invalid numeric value for y: " + values[i] + ". Continuing with null y value.");
        y = null;
      }
    }
    if (i < attributeNames.size())
    {
      attrs.put(attributeNames.get(i), values[i]);
    }
  }

  if (wktGeometry != null)
  {
    try
    {
      feature = GeometryFactory.fromJTS(_wktReader.read(wktGeometry), attrs);
    }
    catch (Exception e)
    {
      //try to correct wktGeometry if possible
      try
      {
        feature = GeometryFactory.fromJTS(_wktReader.read(WktGeometryUtils.wktGeometryFixer(wktGeometry)));
      }
      catch (Exception e2)
      {
        //could not fix the geometry, so just set to null
        log.error("Could not fix geometry: " + wktGeometry + ". Continuing with null geometry.");
      }
    }
  }
  else if (geometryCol == -1 && xCol >= 0 && yCol >= 0)
  {
    if (x != null && y != null)
    {
      feature = GeometryFactory.createPoint(x, y, attrs);
    }
  }

  if (feature == null)
  {
    feature = GeometryFactory.createEmptyGeometry(attrs);
  }

  return feature;
}

@Override
public void writeExternal(ObjectOutput out) throws IOException
{
  if (attributeNames != null)
  {
    out.writeBoolean(true);
    out.writeInt(attributeNames.size());
    for (String name : attributeNames)
    {
      out.writeUTF(name);
    }
  }
  else
  {
    out.writeBoolean(false);
  }
  out.writeInt(xCol);
  out.writeInt(yCol);
  out.writeInt(geometryCol);
  out.writeChar(delimiter);
  out.writeChar(encapsulator);
  out.writeBoolean(skipFirstLine);
}

@Override
public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
{
  boolean hasAttributes = in.readBoolean();
  if (hasAttributes)
  {
    attributeNames = new ArrayList<String>();
    int count = in.readInt();
    for (int i = 0; i < count; i++)
    {
      attributeNames.add(in.readUTF());
    }
  }
  xCol = in.readInt();
  yCol = in.readInt();
  geometryCol = in.readInt();
  delimiter = in.readChar();
  encapsulator = in.readChar();
  skipFirstLine = in.readBoolean();
}
}
