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

package org.mrgeo.utils;

import com.sun.org.apache.xml.internal.serialize.OutputFormat;
import com.sun.org.apache.xml.internal.serialize.XMLSerializer;
import com.sun.org.apache.xpath.internal.XPathAPI;
import org.w3c.dom.Comment;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import java.io.*;


/**
 * @author jason.surratt
 * 
 */
public class XmlUtils
{
  public static Document createDocument() throws IOException
  {
    DocumentBuilderFactory dBF = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder;
    try
    {
      builder = dBF.newDocumentBuilder();
    }
    catch (ParserConfigurationException e)
    {
      throw new IOException("Error creating document builder. (" + e.getMessage() + ")");
    }
    return builder.newDocument();
  }

  public static Element createElement(Node parent, String tagName)
  {
    Document doc;
    if (parent instanceof Document)
    {
      doc = (Document)parent;
    }
    else
    {
      doc = parent.getOwnerDocument();
    }
    Element e = doc.createElement(tagName);
    parent.appendChild(e);
    return e;
  }

  public static Element createTextElement(Element parent, String tagName, double v)
  {
    return createTextElement(parent, tagName, Double.valueOf(v).toString());
  }
  
  public static Element createTextElement(Element parent, String tagName, long v)
  {
    return createTextElement(parent, tagName, Long.valueOf(v).toString());
  }
  
  public static Element createTextElement(Element parent, String tagName, int v)
  {
    return createTextElement(parent, tagName, Integer.valueOf(v).toString());
  }
  
  public static Element createTextElement(Element parent, String tagName, String text)
  {
    Document doc = parent.getOwnerDocument();
    Element e = createElement(parent, tagName);
    if (text == null)
    {
      text = "";
    }
    e.appendChild(doc.createTextNode(text));
    return e;
  }
  
  /**
   * Creates a DOM comment
   * @param parent parent DOM element
   * @param str comment text
   * @return DOM comment
   */
  public static Comment createComment(Element parent, String str)
  {
    Document doc = parent.getOwnerDocument();
    Comment c = doc.createComment(str);
    parent.appendChild(c);
    return c;
  }

  /**
   * Creates a DOM element
   * @param parent parent DOM element
   * @param tagName element name
   * @return a DOM element
   */
  public static Element createElement(Element parent, String tagName)
  {
    Document doc = parent.getOwnerDocument();
    Element e = doc.createElement(tagName);
    parent.appendChild(e);
    return e;
  }

  /**
   * Creates a DOM text element
   * @param parent parent DOM element
   * @param tagName element name
   * @param text element text
   * @return a DOM element
   */
  public static Element createTextElement2(Element parent, String tagName, String text)
  {
    Document doc = parent.getOwnerDocument();
    Element e = doc.createElement(tagName);
    e.appendChild(doc.createTextNode(text));
    parent.appendChild(e);
    return e;
  }

  public static XPath createXPath()
  {
    XPathFactory factory = XPathFactory.newInstance();
    return factory.newXPath();
  }

  /**
   * Returns null if the attribute doesn't exist, otherwise returns the attribute.
   * @param node
   * @param attribute
   * @return
   */
  public static String getAttribute(Node node, String attribute)
  {
    Node attributeNode = node.getAttributes().getNamedItem(attribute);

    if (attributeNode == null)
    {
      return null;
    }
    return node.getNodeValue();
  }

  public static Document parseFile(File fn) throws IOException
  {
    try
    {
      DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
      domFactory.setNamespaceAware(false);
      DocumentBuilder builder = domFactory.newDocumentBuilder();
      return builder.parse(new FileInputStream(fn));
    }
    catch (Exception e)
    {
      throw new IOException("Error parsing XML Stream", e);
    }
  }

  /**
   * @param fdis
   * @return
   * @throws ParserConfigurationException
   * @throws IOException
   * @throws SAXException
   */
  public static Document parseInputStream(InputStream is) throws IOException
  {
    try
    {
      DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
      domFactory.setNamespaceAware(false);
      DocumentBuilder builder = domFactory.newDocumentBuilder();
      return builder.parse(is);
    }
    catch (Exception e)
    {
      throw new IOException("Error parsing XML Stream", e);
    }
  }

  public static Document parseString(String xml) throws SAXException, IOException,
      ParserConfigurationException
  {
    return parseString(xml, true);
  }
  
  public static Document parseString(String xml, boolean namespaceAware) throws SAXException, 
    IOException, ParserConfigurationException
  {
    DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
    domFactory.setNamespaceAware(namespaceAware); // never forget this!
    DocumentBuilder builder;
    builder = domFactory.newDocumentBuilder();
    
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    
    return builder.parse(is);
  }
  
  public static String documentToString(Document doc) throws IOException
  {
    StringWriter writer = new StringWriter();
    writeDocument(doc, writer);
    return writer.toString();
  }

  public static void writeDocument(Document doc, Writer out) throws IOException
  {
    // happy to replace this code w/ the non-deprecated code, but I couldn't get the transformer 
    // approach to work. 
    OutputFormat format = new OutputFormat(doc);
    format.setIndenting(true);
    format.setIndent(2);
    XMLSerializer serializer = new XMLSerializer(out, format);
    serializer.serialize(doc);
  }
  
  /**
   * Determines whether the DOM node satisifying the specified query exists
   * 
   * @param topLevelDomNode Root of the XML DOM to search
   * @param xpathQuery XPATH query defining the search to execute
   * @return true if the DOM node satisfying the XPATH query was found; false otherwise
   * @throws TransformerException 
   * @throws Exception
   */
  public static boolean nodeExists(Node topLevelDomNode, String xpathQuery) 
    throws TransformerException
  {
    return XPathAPI.selectNodeList(topLevelDomNode, xpathQuery).getLength() > 0;
  }
}
