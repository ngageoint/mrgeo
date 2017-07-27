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

package org.mrgeo.resources.raster.about;

import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.mapalgebra.MapOpFactory;
import org.mrgeo.services.Configuration;
import org.mrgeo.utils.ClassLoaderUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.*;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.security.Principal;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;


@Path("/About")
public class AboutResource
{
private static final Logger log = LoggerFactory.getLogger(AboutResource.class);

@Context
UriInfo uriInfo;

@Context
SecurityContext sc;

private Properties props;

@GET
@Produces(MediaType.APPLICATION_XML)
public Response doGet()
{
  boolean debugMode = uriInfo.getQueryParameters().containsKey("debug");
  XMLOutputFactory factory = XMLOutputFactory.newInstance();
  factory.setProperty("javax.xml.stream.isRepairingNamespaces", Boolean.TRUE);
  XMLStreamWriter xmlWriter;
  try
  {
    StringWriter writer = new StringWriter();
    xmlWriter = factory.createXMLStreamWriter(writer);
    xmlWriter.writeStartElement("About");

    URL buildInfoUrl = Thread.currentThread().getContextClassLoader().getResource(
        "org/mrgeo/services/build.info");
    Properties buildInfo = new Properties();
    assert buildInfoUrl != null;
    buildInfo.load(buildInfoUrl.openStream());

    String imageBase = Configuration.getInstance().getProperties().getProperty(MrGeoConstants.MRGEO_HDFS_IMAGE, "");

    xmlWriter.writeAttribute("name", buildInfo.getProperty("name"));
    xmlWriter.writeAttribute("version", buildInfo.getProperty("version"));
    xmlWriter.writeAttribute("imagebase", imageBase);
    String user = null;
    Principal principal = sc.getUserPrincipal();
    if (principal != null)
    {
      user = principal.getName();
    }
    xmlWriter.writeAttribute("user", user != null ? user : "*unknown*");

    xmlWriter.writeStartElement("Properties");
    for (Object o : getConfiguration().entrySet())
    {
      Map.Entry prop = (Map.Entry) o;
      xmlWriter.writeStartElement("Property");
      xmlWriter.writeAttribute("name", (String) prop.getKey());
      xmlWriter.writeAttribute("value", (String) prop.getValue());
      xmlWriter.writeEndElement();
    }
    xmlWriter.writeEndElement();

    xmlWriter.writeStartElement("MapAlgebra");
    //ServiceLoader<MapOpFactory> loader = ServiceLoader.load(MapOpFactory.class);

    for (Tuple3<String, String, String> op : MapOpFactory.describe())
    {
      xmlWriter.writeStartElement("Operation");
      xmlWriter.writeAttribute("name", op._1());
      xmlWriter.writeStartElement("Description");
      xmlWriter.writeCharacters(op._2());
      xmlWriter.writeEndElement();
      xmlWriter.writeStartElement("Usage");
      xmlWriter.writeCharacters(op._3());
      xmlWriter.writeEndElement();
      xmlWriter.writeEndElement();
    }
//      for (MapOpFactory s : loader)
//      {
//        for (String n : s.getMapOpNames())
//        {
//          xmlWriter.writeStartElement("Operation");
//          xmlWriter.writeAttribute("name", n);
//          xmlWriter.writeEndElement();
//        }
//      }
    xmlWriter.writeEndElement();


    if (debugMode)
    {
      xmlWriter.writeStartElement("ClassPath");

      xmlWriter.writeStartElement("SystemClassPath");
      xmlWriter.writeCharacters(System.getProperty("java.class.path", null));
      xmlWriter.writeEndElement();

      xmlWriter.writeStartElement("ClassLoader");
      xmlWriter.writeComment("This is not an exhaustive jar list, but it should be pretty good.");

      for (String s : ClassLoaderUtil.getMostJars())
      {
        xmlWriter.writeStartElement("jar");
        xmlWriter.writeAttribute("url", s);
        xmlWriter.writeEndElement();
      }

      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      Enumeration<URL> urls = classLoader.getResources("");
      while (urls.hasMoreElements())
      {
        URL resource = urls.nextElement();
        xmlWriter.writeStartElement(resource.getProtocol());
        xmlWriter.writeAttribute("url", resource.toString());
        xmlWriter.writeEndElement();
      }

      xmlWriter.writeEndElement();
      xmlWriter.writeEndElement();
    }

    xmlWriter.writeEndElement();
    xmlWriter.flush();
    xmlWriter.close();
    writer.close();
    return Response.status(Response.Status.ACCEPTED).entity(writer.getBuffer().toString()).build();
  }
  catch (IOException | XMLStreamException e)
  {
    log.error("Got exception", e);
    throw new WebApplicationException(
        Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
  }
}

private Properties getConfiguration()
{
  if (props == null)
  {
    try
    {
      props = Configuration.getInstance().getProperties();
    }
    catch (IllegalStateException e)
    {
      log.error("About error: " + e.getMessage(), e);
    }
  }
  return props;
}
}
