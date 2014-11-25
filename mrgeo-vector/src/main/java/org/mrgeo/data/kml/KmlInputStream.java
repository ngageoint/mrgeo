/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.data.kml;

import org.apache.hadoop.fs.Path;
import org.mrgeo.geometry.*;
import org.mrgeo.geometryfilter.JtsConverter;
import org.mrgeo.data.GeometryCollection;
import org.mrgeo.data.GeometryInputStream;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.XmlUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.*;
import java.util.*;


/**
 * @author jason.surratt
 * 
 */
public class KmlInputStream implements GeometryInputStream, GeometryCollection
{
  static class LocalIterator implements Iterator<WritableGeometry>
  {
    private Iterator<Geometry> src;

    public LocalIterator(Iterator<Geometry> src)
    {
      this.src = src;
    }

    @Override
    public boolean hasNext()
    {
      return src.hasNext();
    }

    @Override
    public WritableGeometry next()
    {
      return src.next().createWritableClone();
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }

  private static final long serialVersionUID = 1L;

  private transient Vector<Geometry> allGeometries;
  private transient ListIterator<Geometry> it;
  private transient Path kmlPath = null;
  private String kmlPathStr = null;
  private transient XPath xpath;

  /**
   * If you use this constructor then serialization will throw an exception.
   * 
   * @param is
   * @throws IOException
   */
  public KmlInputStream(InputStream is) throws IOException
  {
    loadKml(is);
  }

  public KmlInputStream(Path src) throws IOException
  {
    kmlPath = src;
    loadKml(src);
  }

  /**
   * Convenience function similar to above.
   * 
   * @param fileName
   * @throws IOException
   */
  public KmlInputStream(String fileName) throws IOException
  {
    FileInputStream fis = new FileInputStream(fileName);
    loadKml(fis);
  }

  @Override
  public WritableGeometry get(int index)
  {
    return allGeometries.get(index).createWritableClone();
  }

  @Override
  public String getProjection()
  {
    return WellKnownProjections.WGS84;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.spadac.Geometry.GeometryInputStream#hasNext()
   */
  @Override
  public boolean hasNext()
  {
    return it.hasNext();
  }

  @Override
  public Iterator<WritableGeometry> iterator()
  {
    return new LocalIterator(allGeometries.iterator());
  }

  /**
   * @param is
   * @throws IOException
   */
  private void loadKml(InputStream is) throws IOException
  {
    try
    {
      DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
      domFactory.setNamespaceAware(false);
      DocumentBuilder builder = domFactory.newDocumentBuilder();
      Document doc = builder.parse(is);
      
      xpath = XmlUtils.createXPath();

      allGeometries = new Vector<Geometry>();

      // Everything in the document
      parseNode(doc);
    }
    catch (Exception e)
    {
      throw new IOException("Error reading KML file.", e);
    }
    reset();
  }

  private void parseNode(Node node) throws XPathExpressionException, IOException
  {
    
    for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
    {
      if (child.getNodeType() == Node.ELEMENT_NODE)
      {
        Element e = (Element)child;
        if (e.getNodeName().equals("Placemark"))
        {
          Geometry g = parsePlacemark(e);
          if (g != null)
          {
            allGeometries.add(g);
          }
        }
        else
        {
          parseNode(e);
        }
      }
    }
  }

  private WritableGeometry parsePlacemark(Node node) throws XPathExpressionException, IOException
  {
    HashMap<String, String> attributes = new HashMap<String, String>();
    WritableGeometry result = null;
    
    for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
    {
      if (child.getNodeType() == Node.ELEMENT_NODE)
      {
        Element e = (Element)child;
        if (e.getNodeName().equals("Point"))
        {
          result = parsePoint(e);
        }
        else if (e.getNodeName().equals("Polygon"))
        {
          result = parsePolygon(e);
        }
        else if (e.getNodeName().equals("LineString"))
        {
          result = parseLineString(e);
        }
        else
        {
          addAttributes(attributes, "", e);
        }
      }
    }
    
    if (result != null)
    {
      for (Map.Entry<String, String> e : attributes.entrySet())
      {
        result.setAttribute(e.getKey(), e.getValue());
      }
    }
    else
    {
      throw new IOException("Did not found a geometry in the placemark.");
    }
    
    return result;
  }

  private void addAttributes(HashMap<String, String> attributes, String prefix, Node node)
  {
    String separator = prefix.length() > 0 ? "/" : "";
    for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
    {
      if (child.getNodeType() == Node.ELEMENT_NODE)
      {
        addAttributes(attributes, prefix + separator + node.getNodeName(), child);
      }
      else if (child.getNodeType() == Node.TEXT_NODE && node.getChildNodes().getLength() == 1)
      {
        Text tn = (Text)child;
        attributes.put(prefix + separator + node.getNodeName(), tn.getData());
      }
    }
  }

  private void loadKml(Path src) throws IOException
  {
    kmlPath = src;
    //FileSystem fs = HadoopFileUtils.getFileSystem(src);
    InputStream fdis = HadoopFileUtils.open(src); // fs.open(src);
    loadKml(fdis);
    fdis.close();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.spadac.Geometry.GeometryInputStream#next()
   */
  @Override
  public WritableGeometry next()
  {
    if (it.hasNext())
    {
      Geometry g = it.next();
      return g.createWritableClone();
    }
    return null;
  }

  private WritableLineString parseLineString(Node ring) throws XPathExpressionException
  {
    String coord = xpath.evaluate("coordinates/text()", ring);

    String[] c = coord.split(" ");

    WritableLineString result = GeometryFactory.createLineString();

    for (int i = 0; i < c.length; i++)
    {
      String[] v = c[i].split(",");
      result.addPoint(GeometryFactory.createPoint(Double.valueOf(v[0]), Double.valueOf(v[1]), Double
          .valueOf(v[2])));
    }

    return result;
  }

  private WritableLinearRing parseLinearRing(Node ring) throws XPathExpressionException
  {
    String coord = xpath.evaluate("coordinates/text()", ring);

    String[] c = coord.split(" ");

    WritableLinearRing result = GeometryFactory.createLinearRing();

    for (int i = 0; i < c.length; i++)
    {
      String[] v = c[i].split(",");
      result.addPoint(GeometryFactory.createPoint(Double.valueOf(v[0]), Double.valueOf(v[1]), Double
          .valueOf(v[2])));
    }

    result.closeRing();

    return result;
  }

  /**
   * @param item
   * @return
   * @throws XPathExpressionException
   */
  private static WritablePoint parsePoint(Node item) throws XPathExpressionException
  {
    String coord = null;
    Node child = item.getFirstChild();
    while (child != null)
    {
      if (child.getNodeName().equals("coordinates") && child instanceof Element)
      {
        Element e = (Element)child;
        coord = e.getFirstChild().getTextContent();
      }
      child = child.getNextSibling();
    }

    if (coord != null)
    {
    String[] c = coord.split(",");

    return GeometryFactory.createPoint(Double.valueOf(c[0]), Double.valueOf(c[1]), Double.valueOf(c[2]));
    }
    
    throw new IllegalArgumentException("Cannot parse point.");
  }

  private WritablePolygon parsePolygon(Node polygon) throws XPathExpressionException
  {
    WritablePolygon result = GeometryFactory.createPolygon();
    Node node = (Node) xpath.evaluate("outerBoundaryIs/LinearRing", polygon, XPathConstants.NODE);

    // TODO Handle inner rings as well.
    WritableLinearRing outerRing = parseLinearRing(node);
    result.setExteriorRing(outerRing);

    return result;
  }

  private void readObject(ObjectInputStream is) throws ClassNotFoundException, IOException
  {
    // always perform the default de-serialization first
    is.defaultReadObject();
    
    if (kmlPathStr == null)
    {
      throw new IllegalArgumentException("Cannot deserialize a KmlInputStream without a path.");
    }
    kmlPath = new Path(kmlPathStr);
    loadKml(kmlPath);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.Iterator#remove()
   */
  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }

  public void reset()
  {
    it = allGeometries.listIterator();
  }

  @Override
  public int size()
  {
    return allGeometries.size();
  }

  private void writeObject(ObjectOutputStream os) throws IOException
  {
    if (kmlPath == null)
    {
      throw new IllegalArgumentException("Cannot serialize a KmlInputStream without a path.");
    }
    kmlPathStr = kmlPath.toString();
    os.defaultWriteObject();
  }
  
  @Override
  public void close()
  {
    // no op
  }

}
