/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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

package org.mrgeo.services.utils;

import org.mrgeo.services.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Utilities used in creating XML document responses for WMS requests
 */
public class DocumentUtils
{
  private static final Logger log = LoggerFactory.getLogger(DocumentUtils.class);

  /**
   * Writes the XML document response to the output
   * @param doc XML document
   * @param version WMS version
   * @param out output writer
   * @throws TransformerException
   */
  public static void writeDocument(Document doc, Version version, String service, PrintWriter out)
    throws TransformerException
  {
    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    Transformer transformer = transformerFactory.newTransformer();
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
//    transformer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM, String.format(
//      "http://schemas.opengis.net/%s/%s/%sAll.xsd", service, service, version.toString()));
    DOMSource source = new DOMSource(doc);
    StreamResult result = new StreamResult(out);
    transformer.transform(source, result);
  }

  /**
   * Checks the provided document against the 1.1.1 DTD
   * @param doc XML document
   * @param version WMS version
   * @throws SAXException
   * @throws IOException
   * @throws TransformerException
   * @throws ParserConfigurationException
   */
  public static void checkForErrors(Document doc, String service, Version version) throws SAXException, IOException,
    TransformerException, ParserConfigurationException
  {
    ByteArrayOutputStream strm = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(strm);
    DocumentUtils.writeDocument(doc, version, service, pw);

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setValidating(true);
    DocumentBuilder builder = factory.newDocumentBuilder();
    builder.setErrorHandler(new ErrorHandler()
    {
      // Validation errors
      @Override
      public void error(SAXParseException ex) throws SAXParseException
      {
        log.error("Error at " + ex.getLineNumber() + " line.");
        log.error(ex.getMessage());
      }

      // Ignore the fatal errors
      @Override
      public void fatalError(SAXParseException ex) throws SAXException
      {
        log.error("Fatal Error at " + ex.getLineNumber() + " line.");
        log.error(ex.getMessage());
      }

      // Show warnings
      @Override
      public void warning(SAXParseException ex) throws SAXParseException
      {
        log.warn("Warning at " + ex.getLineNumber() + " line.");
        log.warn(ex.getMessage());
      }
    });

    ByteArrayInputStream input = new ByteArrayInputStream(strm.toByteArray());

    Document xmlDocument = builder.parse(input);
    DOMSource source = new DOMSource(xmlDocument);
    StreamResult result = new StreamResult(new ByteArrayOutputStream());
    TransformerFactory tf = TransformerFactory.newInstance();
    Transformer transformer = tf.newTransformer();
    //TODO: should version be dynamic here?
    transformer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM, String.format(
        "http://schemas.opengis.net/%s/%s/%sAll.xsd", service, service, version.toString()));
    transformer.transform(source, result);
  }
}
