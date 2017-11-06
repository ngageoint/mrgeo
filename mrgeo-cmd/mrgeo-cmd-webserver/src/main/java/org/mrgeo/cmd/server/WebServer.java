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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.glassfish.jersey.servlet.ServletContainer;
import org.mrgeo.cmd.Command;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.utils.logging.LoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.net.*;
import java.security.KeyStore;
import java.util.Enumeration;

public class WebServer extends Command
{

private static Logger log = LoggerFactory.getLogger(WebServer.class);

public WebServer()
{
}

@Override
public void addOptions(Options options)
{
  Option port = new Option("p", "port", true, "The port on which the server will listen (default 8080)");
  options.addOption(port);

  Option host = new Option("hs", "host", true, "The host on which the server will listen (default ALL interfaces (0.0.0.0))");
  options.addOption(host);

  Option secure = new Option("sc", "secure", false, "Enable the ssl (https) interface.  You MUST at least include the --password option as well");
  options.addOption(secure);

  Option keystore = new Option("ks", "keystore", true, "Name and location of the Java keystore (default $JAVA_HOME/lib/security/cacerts)");
  options.addOption(keystore);

  Option pw = new Option("pw", "password", true, "Keystore password (prefix with \"OBF:\" to use a Jetty obfuscated password)");
  options.addOption(pw);

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
    String host = "0.0.0.0";

    if (line.hasOption("p"))
    {
      try
      {
        httpPort = Integer.parseInt(line.getOptionValue("p"));
      }
      catch (NumberFormatException nfe)
      {
        throw new ParseException("Invalid HTTP port specified: " + line.getOptionValue("p"));
      }
    }

    if (line.hasOption("hs"))
    {
      host = line.getOptionValue("hs");
    }

    boolean singleThreaded = line.hasOption("st");

    boolean secure = line.hasOption("sc");
    String keystore = null;
    String pw = null;

    if (secure)
    {
      if (line.hasOption("ks") && !line.hasOption("pw"))
      {
        throw new ParseException("Must supply a keystore password when supplying a keystore location");
      }

      keystore = line.getOptionValue("ks", null);
      pw = line.getOptionValue("pw", "changeit");
    }


    if (line.hasOption("v"))
    {
      LoggingUtils.setDefaultLogLevel(LoggingUtils.INFO);
    }
    if (line.hasOption("d"))
    {
      LoggingUtils.setDefaultLogLevel(LoggingUtils.DEBUG);
    }

    runWebServer(host, httpPort, singleThreaded, secure, keystore, pw);
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
private Server startWebServer(String host, int httpPort, boolean singleThreaded, boolean secure,
    String keystore, String pw) throws Exception
{
  System.out.println("Starting embedded web server on port " + httpPort);
  Server server = null;

  if (singleThreaded)
  {
    // Based on the connector configuration below (min = 1, max = 1), Jetty requires a
    // minimum thread pool size of three threads for its processing. One acceptor thread,
    // one selector thread, and one request thread. It will queue up requests that it
    // can't immediately process.
    ThreadPool threadPool = new QueuedThreadPool(3, 1);
    server = new Server(threadPool);
  }
  else if (secure)
  {
    server = new Server();
  }
  else
  {
    InetSocketAddress isa = new InetSocketAddress(host, httpPort);
    server = new Server(isa);
  }

  ServerConnector httpsConnector = null;
  if (secure)
  {
    HttpConfiguration http_config = new HttpConfiguration();
    http_config.setSecureScheme("https");
    http_config.setSecurePort(httpPort);

    HttpConfiguration https_config = new HttpConfiguration(http_config);
    https_config.addCustomizer(new SecureRequestCustomizer());

    SslContextFactory sslContextFactory;
    if (keystore == null)
    {
      File defkeystore = new File(System.getenv("JAVA_HOME"), "lib/security/cacerts");

      sslContextFactory = new SslContextFactory(defkeystore.getCanonicalPath());
    }
    else
    {
      sslContextFactory = new SslContextFactory(keystore);
    }
    sslContextFactory.setKeyStorePassword(pw);

    httpsConnector = new ServerConnector(server,
        new SslConnectionFactory(sslContextFactory, "http/1.1"),
        new HttpConnectionFactory(https_config));

    httpsConnector.setPort(httpPort);
    httpsConnector.setHost(host);
  }

  if (singleThreaded)
  {
    System.out.println("  Running in single-threaded mode");
    // Based on the connector configuration below (min = 1, max = 1), Jetty requires a
    // minimum thread pool size of three threads for its processing. One acceptor thread,
    // one selector thread, and one request thread. It will queue up requests that it
    // can't immediately process.
    ServerConnector connector = new ServerConnector(server, 1, 1);

    connector.setPort(httpPort);
    connector.setHost(host);

    if (httpsConnector != null)
    {
      server.setConnectors(new Connector[]{httpsConnector, connector});
    }
    else
    {
      server.setConnectors(new Connector[]{connector});
    }
  }
  else
  {
    System.out.println("  Running in multi-threaded mode");
    if (httpsConnector != null)
    {
      server.setConnectors(new Connector[]{httpsConnector});
    }
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
  return server;
}

@SuppressWarnings("squid:S00112") // Passing on exception thrown from 3rd party API
private void runWebServer(String host, int httpPort, boolean singleThreaded, boolean secure, String keystore,
    String pw) throws Exception
{
  Server server = startWebServer(host, httpPort, singleThreaded, secure, keystore, pw);

  System.out.println("Embedded web server started, listening on port " + httpPort);

  printEndpoints(host, httpPort, server, secure);

  System.out.println("Use ctrl-C to stop the web server");

  server.join();
}

private void printEndpoints(String host, int httpPort, Server server, boolean secure) throws SocketException, URISyntaxException
{
  String protocol;
  if (secure)
  {
    protocol = "https";
  }
  else
  {
    protocol = "http";

  }

  if ("0.0.0.0".contentEquals(host))
  {
    Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
    if (en != null)
    {
      while (en.hasMoreElements())
      {
        NetworkInterface intf = en.nextElement();

        Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses();
        while (enumIpAddr.hasMoreElements())
        {
          InetAddress ipAddr = enumIpAddr.nextElement();

          String hostname = ipAddr.getCanonicalHostName();
          if (hostname.contains("%"))
          {
            hostname = StringUtils.substringBefore(hostname, "%");
          }
          //printInetAddress(ipAddr);
          URI uri = new URI(protocol, null, hostname, httpPort, null, null, null);
          System.out.println("  " + uri);
        }
      }
    }
  }
  else
  {
    String hostname = host;
    if (hostname.contains("%"))
    {
      hostname = StringUtils.substringBefore(hostname, "%");
    }

    URI uri = new URI(protocol, null, hostname, httpPort, null, null, null);
    System.out.println("  " + uri);
  }

}

//private static void printInetAddress(InetAddress myAddress) {
//  System.out.println( "toString: " + myAddress);
//  System.out.println( "hostName: " + myAddress.getHostName());
//  System.out.println( "canonicalHostName: " + myAddress.getCanonicalHostName());
//  System.out.println( "getHostAddress: " + myAddress.getHostAddress());
//}

}
