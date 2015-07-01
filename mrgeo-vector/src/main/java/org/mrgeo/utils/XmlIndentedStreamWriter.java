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

package org.mrgeo.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 * Originally I was using the Sun provided indented stream writer, but that isn't available
 * with all JVM implementations. So, I've written this really basic poor mans version. This 
 * has not been extensively tested so there is a good chance that some scenarios will cause
 * unexpected output, but besides the extraneous indentation it _should_ still be valid XML.
 * 
 * I also looked at bringing in another library to the mix that would implement this function
 * (specifically stax-utils), but I didn't think that this simple class justified adding 
 * another library.
 */
public class XmlIndentedStreamWriter implements XMLStreamWriter
{
  @SuppressWarnings("unused")
  private static final Logger _log = LoggerFactory.getLogger(XmlIndentedStreamWriter.class);

  int _level = 0;
  String _indent;
  XMLStreamWriter _writer;
  String _indentString = "";
  enum WriteType
  {
    Other,
    Data
  }
  WriteType _last = WriteType.Other;

  public XmlIndentedStreamWriter(XMLStreamWriter writer, String indent)
  {
    _indent = indent;
    _writer = writer;
  }

  @Override
  public void close() throws XMLStreamException
  {
    _writer.close();
  }

  protected void _createIndentString()
  {
    StringBuffer buf = new StringBuffer();

    for (int i = 0; i < _level; i++)
    {
      buf.append(_indent);
    }

    _indentString = buf.toString();
  }

  private void _dedent()
  {
    _level--;
    _createIndentString();
  }

  @Override
  public void flush() throws XMLStreamException
  {
    _writer.flush();
  }

  @Override
  public NamespaceContext getNamespaceContext()
  {
    return _writer.getNamespaceContext();
  }

  @Override
  public String getPrefix(String uri) throws XMLStreamException
  {
    return _writer.getPrefix(uri);
  }

  @Override
  public Object getProperty(String name) throws IllegalArgumentException
  {
    return _writer.getProperty(name);
  }

  private void _indent()
  {
    _level++;
    _createIndentString();
  }

  @Override
  public void setDefaultNamespace(String uri) throws XMLStreamException
  {
    _writer.setDefaultNamespace(uri);
  }

  @Override
  public void setNamespaceContext(NamespaceContext context) throws XMLStreamException
  {
    _writer.setNamespaceContext(context);
  }

  @Override
  public void setPrefix(String prefix, String uri) throws XMLStreamException
  {
    _writer.setPrefix(prefix, uri);
  }

  @Override
  public void writeAttribute(String localName, String value) throws XMLStreamException
  {
    _writer.writeAttribute(localName, value);
  }

  @Override
  public void writeAttribute(String namespaceURI, String localName, String value)
      throws XMLStreamException
  {
    _writer.writeAttribute(namespaceURI, localName, value);
  }

  @Override
  public void writeAttribute(String prefix, String namespaceURI, String localName, String value)
      throws XMLStreamException
  {
    _writer.writeAttribute(prefix, namespaceURI, localName, value);
  }

  @Override
  public void writeCData(String data) throws XMLStreamException
  {
    _last = WriteType.Data;
    _writer.writeCData(data);
  }

  @Override
  public void writeCharacters(String text) throws XMLStreamException
  {
    _last = WriteType.Data;
    _writer.writeCharacters(text);
  }

  @Override
  public void writeCharacters(char[] text, int start, int len) throws XMLStreamException
  {
    _last = WriteType.Data;
    _writer.writeCharacters(text, start, len);
  }

  @Override
  public void writeComment(String data) throws XMLStreamException
  {
    _writeIndent();
    _writer.writeComment(data);
    _writeEndLine();
  }

  @Override
  public void writeDTD(String dtd) throws XMLStreamException
  {
    _writeIndent();
    _writer.writeDTD(dtd);
    _writeEndLine();
  }

  @Override
  public void writeDefaultNamespace(String namespaceURI) throws XMLStreamException
  {
    _writeIndent();
    _writer.writeDefaultNamespace(namespaceURI);
    _writeEndLine();
  }

  @Override
  public void writeEmptyElement(String localName) throws XMLStreamException
  {
    _writeIndent();
    _writer.writeEmptyElement(localName);
    _writeEndLine();
  }

  @Override
  public void writeEmptyElement(String namespaceURI, String localName) throws XMLStreamException
  {
    _writeIndent();
    _writer.writeEmptyElement(namespaceURI, localName);
    _writeEndLine();
  }

  @Override
  public void writeEmptyElement(String prefix, String localName, String namespaceURI)
      throws XMLStreamException
  {
    _writeIndent();
    _writer.writeEmptyElement(prefix, localName, namespaceURI);
    _writeEndLine();
  }

  @Override
  public void writeEndDocument() throws XMLStreamException
  {
    _writer.writeEndDocument();
  }

  @Override
  public void writeEndElement() throws XMLStreamException
  {
    _dedent();
    _beforeElement();
    _writer.writeEndElement();
  }

  protected void _writeEndLine() throws XMLStreamException
  {
    _writer.writeCharacters("\n");
  }

  @Override
  public void writeEntityRef(String name) throws XMLStreamException
  {
    _writeIndent();
    _writer.writeEntityRef(name);
    _writeEndLine();
  }

  protected void _writeIndent() throws XMLStreamException
  {
    _writer.writeCharacters(_indentString);
  }

  @Override
  public void writeNamespace(String prefix, String namespaceURI) throws XMLStreamException
  {
    _writeIndent();
    _writer.writeNamespace(prefix, namespaceURI);
    _writeEndLine();
  }

  @Override
  public void writeProcessingInstruction(String target) throws XMLStreamException
  {
    _writeIndent();
    _writer.writeProcessingInstruction(target);
    _writeEndLine();
  }

  @Override
  public void writeProcessingInstruction(String target, String data) throws XMLStreamException
  {
    _writeIndent();
    _writer.writeProcessingInstruction(target, data);
    _writeEndLine();
  }

  @Override
  public void writeStartDocument() throws XMLStreamException
  {
    _writer.writeStartDocument();
  }

  @Override
  public void writeStartDocument(String version) throws XMLStreamException
  {
    _writeIndent();
    _writer.writeStartDocument(version);
    _writeEndLine();
  }

  @Override
  public void writeStartDocument(String encoding, String version) throws XMLStreamException
  {
    _writeIndent();
    _writer.writeStartDocument(encoding, version);
    _writeEndLine();
  }

  @Override
  public void writeStartElement(String localName) throws XMLStreamException
  {
    _beforeElement();
    _writer.writeStartElement(localName);
    _indent();
  }

  @Override
  public void writeStartElement(String namespaceURI, String localName) throws XMLStreamException
  {
    _beforeElement();
    _writer.writeStartElement(namespaceURI, localName);
    _indent();
  }

  @Override
  public void writeStartElement(String prefix, String localName, String namespaceURI) throws XMLStreamException
  {
    _beforeElement();
    _writer.writeStartElement(prefix, localName, namespaceURI);
    _indent();
  }

  private void _beforeElement() throws XMLStreamException
  {
    if (_last == WriteType.Other)
    {
      _writeEndLine();
      _writeIndent();
    }
    _last = WriteType.Other;
  }
}
