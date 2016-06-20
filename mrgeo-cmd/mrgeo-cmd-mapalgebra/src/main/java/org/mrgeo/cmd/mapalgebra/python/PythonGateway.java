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

package org.mrgeo.cmd.mapalgebra.python;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.MrGeo;
import org.mrgeo.data.ProviderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayServer;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

public class PythonGateway extends Command
{
private static final Logger log = LoggerFactory.getLogger(PythonGateway.class);

private boolean hasQueue = false;
private Queue<Integer> portQueue = new ConcurrentLinkedQueue<>();

public static Options createOptions()
{
  Options result = MrGeo.createOptions();

  Option host = new Option("h", "host", true, "Callback hostname");
  host.setRequired(false);
  result.addOption(host);

  Option port = new Option("p", "port", true, "Callback or listen port");
  port.setRequired(false);
  result.addOption(port);

  Option portrange = new Option("pr", "port-range", true, "Port range for mail py4j communications (\"minport-maxport\")");
  portrange.setRequired(false);
  result.addOption(portrange);

  Option remote = new Option("r", "remote", false, "Wait for remote connection");
  remote.setRequired(false);
  result.addOption(remote);

  return result;
}


@Override
public int run(final String[] args, final Configuration conf,
    final ProviderProperties providerProperties)
{

  try
  {
    final Options options = PythonGateway.createOptions();
    CommandLine line;
    final CommandLineParser parser = new PosixParser();
    line = parser.parse(options, args);

    if (line.hasOption("pr"))
    {
      String rangeStr = line.getOptionValue("pr");
      if (rangeStr != null)
      {
        String[] rng = rangeStr.split("-");
        if (rng.length == 2)
        {
          int min = Integer.parseInt(rng[0]);
          int max = Integer.parseInt(rng[1]);

          int minPort = Math.min(min, max);
          int maxPort = Math.max(min, max);

          for (int i = minPort; i <= maxPort; i++)
          {
            portQueue.add(i);
          }
          hasQueue = true;
        }
      }
    }

    if (line.hasOption("h") && line.hasOption("p"))
    {
      String callbackHost = line.getOptionValue("h");
      int callbackPort = Integer.parseInt(line.getOptionValue("p"));

      return localConnection(callbackHost, callbackPort);
    }
    else if (line.hasOption("r") && line.hasOption("p"))
    {
      int listenPort = Integer.parseInt(line.getOptionValue("p"));
      return remoteConnection(listenPort);
    }
    else
    {
      new HelpFormatter().printHelp("python", options);
      return -1;
    }
  }
  catch (ParseException e)
  {
    e.printStackTrace();
    return -1;
  }
}

private int remoteConnection(int listenPort)
{
  try (ServerSocket serverSocket = new ServerSocket(listenPort))
  {
    while (true)
    {
      log.info("Waiting for connection");
      Socket clientSocket = serverSocket.accept();
      log.info("Got connection");

      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

      String clientAddress = in.readLine();
      String clientPortStr = in.readLine();
      int clientPort = Integer.parseInt(clientPortStr);

      setupThreadedServer(clientAddress, clientPort);
      clientSocket.close();
    }
  }
  catch (IOException e)
  {
    log.error("Can not establish listening socket");
    e.printStackTrace();
    return -1;
  }
}

private int localConnection(String callbackHost, int callbackPort)
{
  try
  {
    GatewayServer server = setupSingleServer(callbackHost, callbackPort);
    int port = server.getListeningPort();
    try
    {
      // Exit on EOF or broken pipe to ensure that this process dies when the Python driver dies:
      while (System.in.read() != -1)
      {
        // Do nothing
        //Thread.sleep(10);
      }
    }
    catch (IOException ignored) //  | InterruptedException ignored)
    {
    }
    log.info("Exiting");

    if (hasQueue)
    {
      portQueue.add(port);
    }

    return 0;
  }
  catch (IOException e)
  {
    e.printStackTrace();
    return -1;
  }
}

private GatewayServer setupSingleServer(String callbackHost, int callbackPort) throws IOException
{
  // Start a GatewayServer on an ephemeral port, unless we have a port range
  int port = 0;
  if (hasQueue)
  {
    try
    {
      port = portQueue.remove();
    }
    catch (NoSuchElementException e)
    {
      throw new IOException("PythonGatewayServer is out of available ports, failing)");
    }


  }

  GatewayServer gateway = new GatewayServer(null, port);
  gateway.start();

  int listeningPort = gateway.getListeningPort();
  if (listeningPort == -1)
  {
    throw new IOException("GatewayServer failed to bind");
  }

  log.info("Starting PythonGatewayServer. Communicating on port " + listeningPort);

  sendGatewayPort(callbackHost, callbackPort, listeningPort);

  return gateway;
}

private void setupThreadedServer(final String callbackHost, final int callbackPort) throws IOException
{
  new Thread()
  {
    public void run()
    {
      try
      {
        log.info("Starting thread: " + this.getName() + "(" + this.getId() + ")");

        GatewayServer server = setupSingleServer(callbackHost, callbackPort);
        int port = server.getListeningPort();

        try
        {
          Semaphore semaphore = new Semaphore(1);
          server.addListener(new ServerListener(semaphore));
          // Exit on EOF or broken pipe to ensure that this process dies when the Python driver dies:

          // Wait for the shutdown
          semaphore.acquire();
//          while (System.in.read() != -1)
//          {
//            // Do nothing
//          }
        }
        catch (InterruptedException ignored)
        {
        }

        if (hasQueue)
        {
          portQueue.add(port);
        }

        log.info("Exiting thread: " + this.getName() + "(" + this.getId() + ")");
      }
      catch (IOException e)
      {
        log.error("Error in thread: " + this.getName() + "(" + this.getId() + "), exiting");
        e.printStackTrace();
      }
    }
  }.start();

}

@SuppressFBWarnings(value = "UNENCRYPTED_SOCKET", justification = "Returning comms socket unencrypted, Actual comms encrypted")
private void sendGatewayPort(String callbackHost, int callbackPort, int port) throws IOException
{
  // Communicate the bound port back to the caller via the caller-specified callback port
  log.info("Sending port number (" + port + ") to pymrgeo running at " + callbackHost  + ":" + callbackPort);

  try (Socket callbackSocket = new Socket(callbackHost, callbackPort))
  {
    DataOutputStream dos = new DataOutputStream(callbackSocket.getOutputStream());
    dos.writeInt(port);
    dos.close();
  }
  catch (IOException e)
  {
    throw new IOException("Can not establish callback socket", e);
  }
}

}
