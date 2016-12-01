/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.cmd.server;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.MrGeo;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.utils.logging.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

public class WebServer extends Command
{

private Options options;

private static Logger log = LoggerFactory.getLogger(WebServer.class);

public WebServer()
{
  options = createOptions();
}


public static Options createOptions()
{
  Options result = MrGeo.createOptions();

  Option output = new Option("p", "port", true, "The HTTP port on which the server will listen (default 8080)");
  result.addOption(output);

  return result;
}

@Override
public int run(String[] args, Configuration conf, ProviderProperties providerProperties)
{
  try
  {
    long start = System.currentTimeMillis();

    CommandLine line = null;
    try
    {
      CommandLineParser parser = new GnuParser();
      line = parser.parse(options, args);
    }
    catch (ParseException e)
    {
      System.out.println(e.getMessage());
      new HelpFormatter().printHelp("webserver <options> <operation>", options);
      return -1;
    }

    if (line == null || line.hasOption("h"))
    {
      new HelpFormatter().printHelp("webserver <options> <operation>", options);
      return -1;
    }

    int httpPort = 8080;
    if (line.hasOption("p"))
    {
      try
      {
        httpPort = Integer.parseInt(line.getOptionValue("p", "8080"));
      }
      catch (NumberFormatException nfe)
      {
        System.err.println("Invalid HTTP port specified: " + line.getOptionValue("p"));
        return -1;
      }
    }
    if (line.hasOption("v"))
    {
      LoggingUtils.setDefaultLogLevel(LoggingUtils.INFO);
    }
    if (line.hasOption("d"))
    {
      LoggingUtils.setDefaultLogLevel(LoggingUtils.DEBUG);
    }

    runWebServer(httpPort);

    long end = System.currentTimeMillis();
    long duration = end - start;
    PeriodFormatter formatter = new PeriodFormatterBuilder()
        .appendHours()
        .appendSuffix("h:")
        .appendMinutes()
        .appendSuffix("m:")
        .appendSeconds()
        .appendSuffix("s")
        .toFormatter();
    String formatted = formatter.print(new Period(duration));
    log.info("IngestImage complete in " + formatted);

    return 0;
  }
  catch (Exception e)
  {
    log.error("Exception thrown {}", e);
  }

  return -1;
}

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File() checking for existence")
@SuppressWarnings("squid:S00112") // Passing on exception thrown from 3rd party API
private Server startWebServer(int httpPort) throws Exception
{
  System.out.println("Starting embedded web server on port " + httpPort);
  URI uri = UriBuilder.fromUri("http://" + getHostName() + "/").port(httpPort).build();
  Server server = new Server(httpPort);
  HandlerCollection coll = new HandlerCollection();
  ServletContextHandler context = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
  context.setContextPath("/mrgeo");
  coll.addHandler(context);
  // If the MrGeo configuration defines a static web root path,
  // then add a resource handler for being able to access resources
  // from that path statically from the root context path.
  String webRoot = MrGeoProperties.getInstance().getProperty("web.server.static.root");
  if (webRoot != null && !webRoot.isEmpty())
  {
    boolean goodWebRoot = false;
    File f = new File(webRoot);
    if (f.exists())
    {
      if (f.isDirectory())
      {
        goodWebRoot = true;
      }
      else
      {
        System.out.println("Not serving static web content because web.server.static.root is not a directory: " + webRoot);
      }
    }
    else
    {
      System.out.println("Not serving static web content because web.server.static.root does not exist: " + webRoot);
    }
    if (goodWebRoot)
    {
      System.out.println("Serving static web content from: " + webRoot);
      ResourceHandler rh = new ResourceHandler();
      rh.setDirectoriesListed(true);
      rh.setResourceBase(webRoot);
      coll.addHandler(rh);
    }
  }
  server.setHandler(coll);
  ServletHolder servletHolder = new ServletHolder(new ServletContainer());
  servletHolder.setInitParameter("javax.ws.rs.Application", "org.mrgeo.application.Application");
  //servletHolder.setInitParameter("com.sun.jersey.api.json.POJOMappingFeature", "true");
  servletHolder.setInitOrder(1);
  context.addServlet(servletHolder, "/*");
//    context.addServlet("org.mrgeo.services.wms.WmsGenerator", "/WmsGenerator/*");
  server.start();
  System.out.println(String.format("Web Server started at %s", uri));
  return server;
}

@SuppressWarnings("squid:S00112") // Passing on exception thrown from 3rd party API
private void runWebServer(int httpPort) throws Exception
{
  Server server = startWebServer(httpPort);
  System.out.println("Use ctrl-C to stop the web server");
  server.join();
}

private String getHostName()
{
  try
  {
    return InetAddress.getLocalHost().getCanonicalHostName();
  }
  catch (UnknownHostException e)
  {
    log.error("Exception thrown {}", e);
    System.err.println("Unknown host");
  }
  return "localhost";
}
}
