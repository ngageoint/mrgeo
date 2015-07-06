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

package org.mrgeo.resources.about;

import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.mapalgebra.MapAlgebraParser;
import org.mrgeo.services.Configuration;
import org.mrgeo.services.SecurityUtils;
import org.mrgeo.utils.ClassLoaderUtil;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import java.io.StringWriter;
import java.net.URL;
import java.security.Principal;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;


@Path("/About")
public class AboutResource
{
  private static final long serialVersionUID = 1L;
  private static final Logger log = LoggerFactory.getLogger(AboutResource.class);

  @Context
  UriInfo uriInfo;

  @Context
  SecurityContext sc;

  private Properties props;

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
      Iterator it = getConfiguration().entrySet().iterator();
      while (it.hasNext())
      {
        Map.Entry prop = (Map.Entry)it.next();
        xmlWriter.writeStartElement("Property");
        xmlWriter.writeAttribute("name", (String)prop.getKey());
        xmlWriter.writeAttribute("value", (String)prop.getValue());
        xmlWriter.writeEndElement();
      }
      xmlWriter.writeEndElement();

      xmlWriter.writeStartElement("MapAlgebra");
      //ServiceLoader<MapOpFactory> loader = ServiceLoader.load(MapOpFactory.class);

      // TODO: The provider properties passed to the MapAlgebraParser need to be
      // constructed from calling a security layer with the web context. The security
      // layer should extract whatever it needs from the web request (like a PKI) and
      // set it into the properties so it can be used as needed by the providers.
      MapAlgebraParser p = new MapAlgebraParser(HadoopUtils.createConfiguration(), "",
                                                SecurityUtils.getProviderProperties());
      for (String n : p.getMapOpNames())
      {
        xmlWriter.writeStartElement("Operation");
        xmlWriter.writeAttribute("name", n);
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
    catch (Exception e)
    {
      log.error("Got exception", e);
      throw new WebApplicationException(
              Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build() );
    }
  }
}
