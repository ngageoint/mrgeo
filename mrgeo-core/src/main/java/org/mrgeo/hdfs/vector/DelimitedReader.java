/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.hdfs.vector;

import org.mrgeo.data.CloseableKVIterator;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.geometry.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

/**
 * This class implements the CloseableKVIterator for a delimited text file
 * data source. The iterator's key is a FeatureIdWritable,
 * which maps to a feature id, and it's value is Geometry, which contains
 * the geometry and attributes.
 * <p>
 * The DelimitedReader can be configured with a visitor when constructed
 * that allows for filtering of features as it iterates through them. This
 * class uses a LineProducer to obtain input lines to be converted into
 * Geometry objects.
 */
public class DelimitedReader implements CloseableKVIterator<FeatureIdWritable, Geometry>
{
static final Logger log = LoggerFactory.getLogger(DelimitedReader.class);

private LineProducer lineProducer;
private FeatureIdWritable key = new FeatureIdWritable(-1);
private Geometry feature;
private String currLine;
private boolean currLineAvailable;
private DelimitedParser delimitedParser;
private DelimitedReaderVisitor visitor;
private boolean stopReading = false;
private long featureId = 0L;
private boolean firstLine = true;

public DelimitedReader(LineProducer lineProducer, DelimitedParser delimitedParser)
{
  this.lineProducer = lineProducer;
  this.delimitedParser = delimitedParser;
}

public DelimitedReader(LineProducer lineProducer, DelimitedParser delimitedParser,
    DelimitedReaderVisitor visitor)
{
  this(lineProducer, delimitedParser);
  this.visitor = visitor;
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

private static String nextLine(LineProducer lineProducer) throws IllegalArgumentException
{
  try
  {
    return lineProducer.nextLine();
  }
  catch (IOException e1)
  {
    log.error("Got IOException while reading delimited file: " + lineProducer.toString());
    throw new IllegalArgumentException(e1);
  }
}

@Override
public boolean hasNext()
{
  cacheNextLine();
  return (currLine != null);
}

@Override
public Geometry next()
{
  feature = null;
  key.set(-1);

  while (!stopReading && feature == null)
  {
    cacheNextLine();
    if (currLine != null)
    {
      // We're processing this line, so setup for reading another line later
      currLineAvailable = false;
      Geometry geom = delimitedParser.parse(currLine);
      featureId++;
      if (visitor == null || visitor.accept(featureId, geom))
      {
        feature = geom;
        key.set(featureId);
      }
      if (visitor != null && visitor.stopReading(featureId, geom))
      {
        stopReading = true;
      }
    }
    else
    {
      // No more features to read
      stopReading = true;
    }
  }
  return feature;
}

@Override
public FeatureIdWritable currentKey()
{
  return key;
}

@Override
public Geometry currentValue()
{
  return feature;
}

@Override
public void remove()
{
  // Not supported
}

@Override
public void close() throws IOException
{
  if (lineProducer != null)
  {
    lineProducer.close();
  }
}

private void cacheNextLine()
{
  if (!currLineAvailable)
  {
    String line = null;
    if (!stopReading)
    {
      line = nextLine(lineProducer);
      // Skip the first line if required
      if (firstLine)
      {
        firstLine = false;
        if (delimitedParser.getSkipFirstLine())
        {
          line = nextLine(lineProducer);
        }
      }
      // skip any empty lines as though they don't exist.
      while (line != null && line.isEmpty())
      {
        line = nextLine(lineProducer);
      }
    }
    currLineAvailable = true;
    currLine = line;
  }
}

public interface DelimitedReaderVisitor
{
  public boolean accept(long id, Geometry geometry);

  public boolean stopReading(long id, Geometry geometry);
}
}
