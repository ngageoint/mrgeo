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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mrgeo.hdfs.vector.Column.FactorType;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.XmlUtils;
import org.w3c.dom.Comment;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.List;
import java.util.Vector;

public class ColumnDefinitionFile
{
Vector<Column> columns = new Vector<Column>();

boolean firstLineHeader = false;

public ColumnDefinitionFile()
{
}


public ColumnDefinitionFile(InputStream is) throws IOException
{
  load(is);
}

public ColumnDefinitionFile(Path path) throws IOException
{
  if (path == null)
  {
    throw new IOException("A column file path must be specified.");
  }
  FSDataInputStream fdis = path.getFileSystem(HadoopUtils.createConfiguration()).open(path);
  load(fdis);
  fdis.close();
}

public ColumnDefinitionFile(String path) throws IOException
{
  if (path == null)
  {
    throw new IOException("A column file path must be specified.");
  }
  Path p = new Path(path);

  FSDataInputStream fdis = p.getFileSystem(HadoopUtils.createConfiguration()).open(p);
  load(fdis);
  fdis.close();
}

public Column getColumn(String c) throws IllegalArgumentException
{
  return columns.get(getColumnIndex(c));
}

public int getColumnIndex(String columnName) throws IllegalArgumentException
{
  for (int i = 0; i < columns.size(); i++)
  {
    if (columns.get(i).getName().equals(columnName))
    {
      return i;
    }
  }
  throw new IllegalArgumentException("column not found. " + columnName);
}

public Vector<Column> getColumns()
{
  return columns;
}

public void setColumns(Collection<Column> columns)
{
  this.columns = new Vector<Column>();
  this.columns.addAll(columns);
}

public void setColumns(List<String> names)
{
  this.columns = new Vector<>();
  for (String name : names)
  {
    Column col = new Column(name, FactorType.Unknown);
    columns.add(col);
  }
}

public boolean isFirstLineHeader()
{
  return firstLineHeader;
}

public void setFirstLineHeader(boolean header)
{
  firstLineHeader = header;
}

public void store(OutputStream os) throws IOException
{
  Document doc = XmlUtils.createDocument();

  Element root = doc.createElement("AllColumns");
  root.setAttribute("firstLineHeader", firstLineHeader ? "true" : "false");
  doc.appendChild(root);

  StringBuilder pigLoad = new StringBuilder();

  pigLoad.append("loaded = LOAD 'filename' USING PigStorage() AS (");
  String comma = "";

  for (Column c : columns)
  {
    pigLoad.append(comma);
    pigLoad.append(c.getName());
    comma = ", ";
    Element node = doc.createElement("Column");
    root.appendChild(node);
    node.setAttribute("name", c.getName());
    node.setAttribute("type", c.getType().toString());
    if (c.getCount() > 0)
    {
      node.setAttribute("count", Long.toString(c.getCount()));
    }
    if (c.getType() == FactorType.Numeric)
    {
      if (c.getSum() > 0.0)
      {
        node.setAttribute("sum", Double.toString(c.getSum()));
      }
      if (c.getMin() <= c.getMax())
      {
        node.setAttribute("min", Double.toString(c.getMin()));
        node.setAttribute("max", Double.toString(c.getMax()));
      }
      if (c.isQuartile1Valid())
      {
        node.setAttribute("quartile1", Double.toString(c.getQuartile1()));
      }
      if (c.isQuartile2Valid())
      {
        node.setAttribute("quartile2", Double.toString(c.getQuartile2()));
      }
      if (c.isQuartile3Valid())
      {
        node.setAttribute("quartile3", Double.toString(c.getQuartile3()));
      }
    }
  }

  pigLoad.append(");");

  Comment c = doc.createComment(pigLoad.toString());
  root.appendChild(c);

  XmlUtils.writeDocument(doc, new OutputStreamWriter(os));
}

public void store(Path output) throws IOException
{
  FileSystem fs = output.getFileSystem(HadoopUtils.createConfiguration());
  fs.delete(output, true);
  OutputStream os = fs.create(output, true);
  store(os);
  os.close();
}

private void load(InputStream is) throws IOException
{
  Document doc = XmlUtils.parseInputStream(is);
  XPath xp = XmlUtils.createXPath();

  columns.clear();

  try
  {
    String firstLineHeaderStr = xp.evaluate("/AllColumns/@firstLineHeader", doc);
    if (firstLineHeaderStr != null && firstLineHeaderStr.equals("true"))
    {
      firstLineHeader = true;
    }

    NodeList nodes = (NodeList) xp.evaluate("/AllColumns/Column", doc, XPathConstants.NODESET);
    for (int i = 0; i < nodes.getLength(); i++)
    {
      Element s = (Element) nodes.item(i);
      Column c = new Column();
      c.setName(s.getAttribute("name"));
      String type = s.getAttribute("type");
      if (type.isEmpty())
      {
        type = FactorType.Unknown.toString();
      }
      c.setType(Column.FactorType.valueOf(type));
      String tmp = s.getAttribute("min");
      if (!tmp.isEmpty())
      {
        c.setMin(Double.valueOf(tmp));
      }
      tmp = s.getAttribute("max");
      if (!tmp.isEmpty())
      {
        c.setMax(Double.valueOf(tmp));
      }
      tmp = s.getAttribute("count");
      if (!tmp.isEmpty())
      {
        c.setCount(Long.parseLong(tmp));
      }
      tmp = s.getAttribute("sum");
      if (!tmp.isEmpty())
      {
        c.setSum(Double.valueOf(tmp));
      }
      tmp = s.getAttribute("quartile1");
      if (!tmp.isEmpty())
      {
        c.setQuartile1(Double.valueOf(tmp));
      }
      tmp = s.getAttribute("quartile2");
      if (!tmp.isEmpty())
      {
        c.setQuartile2(Double.valueOf(tmp));
      }
      tmp = s.getAttribute("quartile3");
      if (!tmp.isEmpty())
      {
        c.setQuartile3(Double.valueOf(tmp));
      }
      columns.add(c);
    }
  }
  catch (XPathExpressionException e)
  {
    throw new IOException("Error parsing XML", e);
  }
}

}
