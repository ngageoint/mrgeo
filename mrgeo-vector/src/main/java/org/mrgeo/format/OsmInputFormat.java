/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.format;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.mrgeo.format.XmlInputFormat.XmlRecordReader;
import org.mrgeo.geometry.*;
import org.mrgeo.mapreduce.GeometryWritable;
import org.mrgeo.utils.XmlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Reads nodes, ways and relations from an XML document
 * 
 * This class assumes that there are no CDATA sections to the data.
 */
public class OsmInputFormat extends InputFormat<LongWritable, GeometryWritable> implements
    Serializable
{
  static final Logger log = LoggerFactory.getLogger(OsmInputFormat.class);

  private static final long serialVersionUID = 1L;

  XmlInputFormat xif = new XmlInputFormat();

  @Override
  public RecordReader<LongWritable, GeometryWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException
  {
    OsmRecordReader fr = new OsmRecordReader();
    fr.initialize(split, context);
    return fr;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    List<InputSplit> result = xif.getSplits(context);
    return result;
  }

  static public class OsmRecordReader extends RecordReader<LongWritable, GeometryWritable>
  {
    private LongWritable key = new LongWritable(-1);
    private GeometryWritable value = new GeometryWritable();
    FileSplit split;
    // we initialize to avoid a bunch of if (reader != null) code. It will be recreated in the
    // initialize function.
    XmlRecordReader reader = new XmlRecordReader();
    XPath xp = XmlUtils.createXPath();

    OsmRecordReader()
    {
      
    }

    @Override
    public void close() throws IOException
    {
      reader.close();
    }

    @Override
    public float getProgress() throws IOException
    {
      return reader.getProgress();
    }

    @Override
    public void initialize(InputSplit splt, TaskAttemptContext context) throws IOException
    {
      reader = new XmlRecordReader();
      reader.setPatternString("changeset|node|way|relation");
      reader.initialize(splt, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException
    {
      String xml = "";
      boolean result = false;

      WritableGeometry geometry = null;

      if (reader.nextKeyValue() == true)
      {
        try
        {
          xml = reader.getCurrentValue().toString();
          Document doc = XmlUtils.parseString(xml);
          Node root = doc.getChildNodes().item(0);

          if (root.getNodeName() == "changeset")
          {
            geometry = processChangeset(root);
          }
          else if (root.getNodeName() == "node")
          {
            geometry = processNode(root);
          }
          else if (root.getNodeName() == "way")
          {
            geometry = processWay(root);
          }
          else if (root.getNodeName() == "relation")
          {
            geometry = processRelation(root);
          }
          else
          {
            throw new IOException("Unexpected node name: " + root.getNodeName());
          }

          geometry.setAttribute("__osm_type__", root.getNodeName());

          value.set(geometry);

          result = true;
        }
        catch (Exception e)
        {
          log.warn(xml);
          throw new IOException(e.getMessage(), e);
        }
      }
      return result;
    }

    private WritableGeometry processChangeset(Node root)
    {
      double minX = Double.NaN, minY = Double.NaN, maxX = Double.NaN, maxY = Double.NaN;

      WritablePolygon result = GeometryFactory.createPolygon();

      NamedNodeMap map = root.getAttributes();
      for (int i = 0; i < map.getLength(); i++)
      {
        Node n = map.item(i);
        String k = n.getNodeName();
        String v = n.getNodeValue();
        if (k.equals("min_lon"))
        {
          minX = Double.parseDouble(v);
        }
        if (k.equals("max_lon"))
        {
          maxX = Double.parseDouble(v);
        }
        if (k.equals("min_lat"))
        {
          minY = Double.parseDouble(v);
        }
        if (k.equals("max_lat"))
        {
          maxY = Double.parseDouble(v);
        }
        result.setAttribute(n.getNodeName(), n.getNodeValue());
      }

      result.setExteriorRing(GeometryFactory.createLinearRing(GeometryFactory.createPoint(minX, minY),
        GeometryFactory.createPoint(minX, maxY), GeometryFactory.createPoint(maxX, maxY), 
        GeometryFactory.createPoint(maxX, minY), GeometryFactory.createPoint(minX, minY)));

      addTags(root, result);

      return result;
    }

    private void addTags(Node root, WritableGeometry result)
    {
      NodeList nodes = root.getChildNodes();
      for (int i = 0; i < nodes.getLength(); i++)
      {
        Node n = nodes.item(i);
        if (n.getNodeName().equals("tag"))
        {
          String k = n.getAttributes().getNamedItem("k").getNodeValue();
          String v = n.getAttributes().getNamedItem("v").getNodeValue();
          result.setAttribute("tag:" + k, v);
        }
      }
    }

    private WritableGeometry processNode(Node root)
    {
      WritablePoint result = GeometryFactory.createPoint();

      NamedNodeMap map = root.getAttributes();
      for (int i = 0; i < map.getLength(); i++)
      {
        Node n = map.item(i);
        String name = n.getNodeName();
        String val = n.getNodeValue();

        if (name.equals("lat"))
        {
          result.setY(Double.parseDouble(val));
        }
        else if (name.equals("lon"))
        {
          result.setX(Double.parseDouble(val));
        }
        else
        {
          result.setAttribute(n.getNodeName(), n.getNodeValue());
        }
      }

      addTags(root, result);

      return result;
    }

    private WritableGeometry processRelation(Node root)
    {
      WritableGeometryCollection result = GeometryFactory.createGeometryCollection();

      NamedNodeMap map = root.getAttributes();
      for (int i = 0; i < map.getLength(); i++)
      {
        Node n = map.item(i);
        String name = n.getNodeName();
        String val = n.getNodeValue();

        result.setAttribute(name, val);
      }

      StringBuffer members = new StringBuffer();
      String sep = "";
      NodeList nodes = root.getChildNodes();
      for (int i = 0; i < nodes.getLength(); i++)
      {
        Node n = nodes.item(i);
        if (n.getNodeName().equals("member"))
        {
          String type = n.getAttributes().getNamedItem("type").getNodeValue();
          String ref = n.getAttributes().getNamedItem("ref").getNodeValue();
          String role = n.getAttributes().getNamedItem("role").getNodeValue();
          members.append(sep);
          members.append(type + ":" + ref);
          if (role.isEmpty() == false)
          {
            members.append(",role:" + role);
          }
          sep = "|";
        }
      }
      result.setAttribute("members", members.toString());

      addTags(root, result);

      return result;
    }

    private WritableGeometry processWay(Node root)
    {
      WritableLineString result = GeometryFactory.createLineString();

      NamedNodeMap map = root.getAttributes();
      for (int i = 0; i < map.getLength(); i++)
      {
        Node n = map.item(i);
        String name = n.getNodeName();
        String val = n.getNodeValue();

        result.setAttribute(name, val);
      }

      StringBuffer nodesStr = new StringBuffer();
      String sep = "";
      NodeList nodes = root.getChildNodes();
      for (int i = 0; i < nodes.getLength(); i++)
      {
        Node n = nodes.item(i);
        if (n.getNodeName().equals("nd"))
        {
          String ref = n.getAttributes().getNamedItem("ref").getNodeValue();
          nodesStr.append(sep);
          nodesStr.append("node:" + ref);
          sep = "|";
        }
      }
      result.setAttribute("nodes", nodesStr.toString());

      addTags(root, result);

      return result;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException
    {
      return key;
    }

    @Override
    public GeometryWritable getCurrentValue() throws IOException, InterruptedException
    {
      return value;
    }
  }
}
