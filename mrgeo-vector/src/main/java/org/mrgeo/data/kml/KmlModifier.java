/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.data.kml;

import org.mrgeo.utils.XmlUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;


/**
 * @author jason.surratt
 * 
 */
public class KmlModifier
{
  /**
   * Sets the altitude of all Polygons
   * 
   * @throws XPathExpressionException
   */
  public static void setAltitude(Document doc, double altitude, String altitudeMode)
      throws XPathExpressionException
  {
    XPath xpath = XmlUtils.createXPath();
    // only polygons are supported at this time
    XPathExpression expr = xpath.compile("/kml/*/Polygon");
    NodeList nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
    for (int i = 0; i < nodes.getLength(); i++)
    {
      Node polygonNode = nodes.item(i);
      setChild(polygonNode, "altitude", new Double(altitude).toString());
      if (altitudeMode != null)
      {
        setChild(polygonNode, "altitudeMode", altitudeMode);
      }
    }
    throw new XPathExpressionException("I haven't been tested!");
  }

  /**
   * Sets a nodes child to have a given value. If the child doesn't exist it is
   * created.
   * 
   * @param parent
   * @param childName
   * @param childValue
   * @throws XPathExpressionException
   */
  public static void setChild(Node parent, String childName, String childValue)
      throws XPathExpressionException
  {
    Document doc = parent.getOwnerDocument();
    XPath xpath = XmlUtils.createXPath();
    Node childNode = (Node) xpath.evaluate(childName, parent, XPathConstants.NODE);
    if (childNode == null)
    {
      childNode = doc.createElement(childName);
      parent.appendChild(childNode);
    }
    childNode.setTextContent(childValue);
  }
}
