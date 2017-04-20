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

package org.mrgeo.cmd.mapalgebra.python;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.MrGeo;
import org.mrgeo.data.ProviderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayServer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PythonGateway extends Command
{
private static final Logger log = LoggerFactory.getLogger(PythonGateway.class);

Queue<Integer> portQueue = null;

@Override
public String getUsage() { return "python <options>"; }

@Override
public void addOptions(Options options)
{
  Option port = new Option("p", "port", true, "Listen port (used for remote operation");
  port.setRequired(true);
  options.addOption(port);

  Option portrange =
          new Option("pr", "port-range", true, "Port range for mail py4j communications (\"minport-maxport\")");
  portrange.setRequired(false);
  options.addOption(portrange);
}

@Override
public int run(final CommandLine line, final Configuration conf,
    final ProviderProperties providerProperties) throws ParseException
{

//  try
//  {
//    System.out.print("sleeping...");
//    Thread.sleep(10000);
//    System.out.println(" done");
//  }
//  catch (InterruptedException e)
//  {
//    e.printStackTrace();
//  }

  if (line.hasOption("pr"))
  {
    String rangeStr = line.getOptionValue("pr");
    if (rangeStr != null)
    {
      String[] rng = rangeStr.split("-");
      if (rng.length == 2)
      {
        portQueue = new ConcurrentLinkedQueue<>();

        int min = Integer.parseInt(rng[0]);
        int max = Integer.parseInt(rng[1]);

        int minPort = Math.min(min, max);
        int maxPort = Math.max(min, max);

        for (int i = minPort; i <= maxPort; i++)
        {
          portQueue.add(i);
        }
      }
    }
  }

  int listenPort = Integer.parseInt(line.getOptionValue("p"));
  return remoteConnection(listenPort);
}

private int remoteConnection(int listenPort)
{
  try (ServerSocket serverSocket = new ServerSocket(listenPort))
  {
    // make sure to keep this print on stdout.  The python process uses it in "local" mode for initialization
    System.out.println("Starting...");
    System.out.flush();
    while (true)
    {
      Socket clientSocket = serverSocket.accept();

      setupThreadedServer(clientSocket);

      //clientSocket.close();
    }
  }
  catch (IOException e)
  {
    log.error("Can not establish listening socket {}", e);
    return -1;
  }
}

@SuppressWarnings("squid:S1313") // Hardcoded IP gets local hostname...
private GatewayServer setupSingleServer(Socket clientSocket) throws IOException
{
  // Start a GatewayServer on an ephemeral port, unless we have a port range
  int javaPythonPort = GatewayServer.DEFAULT_PORT;
  int pythonJavaPort = GatewayServer.DEFAULT_PYTHON_PORT;
  if (portQueue != null)
  {
    try
    {
      javaPythonPort = portQueue.remove();
    }
    catch (NoSuchElementException e)
    {
      throw new IOException("PythonGatewayServer is out of available ports, failing)", e);
    }
    try
    {
      pythonJavaPort = portQueue.remove();
    }
    catch (NoSuchElementException e)
    {
      throw new IOException("PythonGatewayServer is out of available ports, failing)", e);
    }
  }


//  public GatewayServer(Object entryPoint, int port, int pythonPort,
//  java.net.InetAddress address, InetAddress pythonAddress, int connectTimeout,
//  int readTimeout, List<Class<? extends py4j.commands.Command>> customCommands) {
  InetAddress javaPythonAddr = InetAddress.getByName("0.0.0.0");
  InetAddress pythonJavaAddr = InetAddress.getByName("0.0.0.0");

  GatewayServer gateway = new GatewayServer(null, javaPythonPort, pythonJavaPort,
      javaPythonAddr, pythonJavaAddr, 0, 0, null);

  PythonGatewayListener listener = new PythonGatewayListener(gateway, clientSocket, javaPythonPort, pythonJavaPort);
  gateway.addListener(listener);

  return gateway;
}

private void setupThreadedServer(final Socket clientSocket) throws IOException
{

  new Thread()
  {
    public void run()
    {
      try
      {
        log.info("Starting thread: " + this.getName() + "(" + this.getId() + ")");

        GatewayServer server = setupSingleServer(clientSocket);

        // start the server inline (not threaded)
        server.start(false);

        int port = server.getListeningPort();
        if (portQueue != null)
        {
          portQueue.add(port);
        }

        log.info("Exiting thread: " + this.getName() + "(" + this.getId() + ")");
      }
      catch (IOException e)
      {
        log.error("Error in thread: " + this.getName() + "(" + this.getId() + "), exiting");
        log.error("Exception thrown", e);
      }
    }
  }.start();
}
}
