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

import org.apache.hadoop.io.LongWritable;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.WritableGeometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reads nodes, ways and relations from an XML document
 * 
 * This class assumes that there are no CDATA sections to the data.
 */
public class OsmContentHandler extends SaxContentHandler<LongWritable, Geometry> implements
    Serializable
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(OsmContentHandler.class);

  private static final long serialVersionUID = 1L;

  private LongWritable key = new LongWritable(-1);
  private WritableGeometry geom;
  private TreeMap<String, String> attr = new TreeMap<String, String>();
  
  enum Type {
    Node,
    Way,
    Relation,
    Changeset
  }
  Type type;

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes)
  {
    if (localName.equals("node"))
    {
      type = Type.Node;
      
      double x = Double.valueOf(attributes.getValue("lon"));
      double y = Double.valueOf(attributes.getValue("lat"));
      attr.put("id", "node:" + attributes.getValue("id"));
      geom = GeometryFactory.createPoint(x, y);
    }
    if (localName.equals("tag"))
    {
      attr.put("tag:" + attributes.getValue("k"), attributes.getValue("v"));
    }
  }
  
  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException
  {
    if (localName.equals("node"))
    {
      WritableGeometry value = geom.createWritableClone();
      value.setAttributes(attr);

      attr.clear();
      try
      {
        // this will throw DoneSaxException if we're all done processing.
        addPair(key, value);
      }
      catch (IOException e)
      {
        throw new SAXException(e);
      }
    }
  }
}
