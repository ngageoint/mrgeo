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

package org.mrgeo.cmd.server;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.glassfish.jersey.servlet.ServletContainer;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.mrgeo.cmd.Command;
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

private static Logger log = LoggerFactory.getLogger(WebServer.class);

public WebServer()
{
}

@Override
public void addOptions(Options options)
{
  Option port = new Option("p", "port", true, "The HTTP port on which the server will listen (default 8080)");
  options.addOption(port);
  Option singleThreaded = new Option("st", "singleThreaded", false, "Specify this argument in order to run the web server in essentially single-threaded mode for processing one request at a time");
  options.addOption(singleThreaded);
}

@Override
public String getUsage() { return "webserver <options>"; }

@Override
public int run(CommandLine line, Configuration conf,
               ProviderProperties providerProperties) throws ParseException
{
  try
  {
    int httpPort = 8080;
    if (line.hasOption("p"))
    {
      try
      {
        httpPort = Integer.parseInt(line.getOptionValue("p", "8080"));
      }
      catch (NumberFormatException nfe)
      {
        throw new ParseException("Invalid HTTP port specified: " + line.getOptionValue("p"));
      }
    }
    boolean singleThreaded = false;
    try
    {
      singleThreaded = line.hasOption("st");
    }
    catch (NumberFormatException nfe)
    {
      throw new ParseException("Invalid number of connections specified: " + line.getOptionValue("n"));
    }

    if (line.hasOption("v"))
    {
      LoggingUtils.setDefaultLogLevel(LoggingUtils.INFO);
    }
    if (line.hasOption("d"))
    {
      LoggingUtils.setDefaultLogLevel(LoggingUtils.DEBUG);
    }

    runWebServer(httpPort, singleThreaded);

    return 0;
  }
  catch (Exception e)
  {
    log.error("Exception thrown", e);
  }

  return -1;
}

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "File() checking for existence")
@SuppressWarnings("squid:S00112") // Passing on exception thrown from 3rd party API
private Server startWebServer(int httpPort, boolean singleThreaded) throws Exception
{
  System.out.println("Starting embedded web server on port " + httpPort);
  URI uri = UriBuilder.fromUri("http://" + getHostName() + "/").port(httpPort).build();
  Server server = null;
  if (singleThreaded) {
    System.out.println("  Running in single-threaded mode");
    // Based on the connector configuration below (min = 1, max = 1), Jetty requires a
    // minimum thread pool size of three threads for its processing. One acceptor thread,
    // one selector thread, and one request thread. It will queue up requests that it
    // can't immediately process.
    ThreadPool threadPool = new QueuedThreadPool(3, 1);
    server = new Server(threadPool);
    ServerConnector connector = new ServerConnector(server, 1, 1);
//    connector.setAcceptQueueSize(0);
    connector.setPort(httpPort);
    server.setConnectors(new Connector[]{connector});
  }
  else {
    server = new Server(httpPort);
  }
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
        System.out
            .println("Not serving static web content because web.server.static.root is not a directory: " + webRoot);
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
private void runWebServer(int httpPort, boolean singleThreaded) throws Exception
{
  Server server = startWebServer(httpPort, singleThreaded);
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
    log.error("Exception thrown", e);
    System.err.println("Unknown host");
  }
  return "localhost";
}
}
