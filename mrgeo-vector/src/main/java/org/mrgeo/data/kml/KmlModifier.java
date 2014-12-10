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
