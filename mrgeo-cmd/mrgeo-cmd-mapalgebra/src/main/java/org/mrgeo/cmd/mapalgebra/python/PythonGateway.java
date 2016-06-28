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

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.MrGeo;
import org.mrgeo.data.ProviderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayServer;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

public class PythonGateway extends Command
{
private static final Logger log = LoggerFactory.getLogger(PythonGateway.class);

private Queue<Integer> portQueue = null;

public static Options createOptions()
{
  Options result = MrGeo.createOptions();

  Option port = new Option("p", "port", true, "Listen port (used for remote operation");
  port.setRequired(true);
  result.addOption(port);

  Option portrange = new Option("pr", "port-range", true, "Port range for mail py4j communications (\"minport-maxport\")");
  portrange.setRequired(false);
  result.addOption(portrange);

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
    // make sure to keep this print on stdout.  The python process uses it in "local" mode for initialization
    System.out.println("Starting...");
    while (true)
    {
      Socket clientSocket = serverSocket.accept();

      setupThreadedServer(clientSocket);

      //clientSocket.close();
    }
  }
  catch (IOException e)
  {
    log.error("Can not establish listening socket");
    e.printStackTrace();
    return -1;
  }
}

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
      throw new IOException("PythonGatewayServer is out of available ports, failing)");
    }
    try
    {
      pythonJavaPort = portQueue.remove();
    }
    catch (NoSuchElementException e)
    {
      throw new IOException("PythonGatewayServer is out of available ports, failing)");
    }
  }

  //GatewayServer gateway = new GatewayServer(null, javaPythonPort);

//  public GatewayServer(Object entryPoint, int port, int pythonPort,
//  java.net.InetAddress address, InetAddress pythonAddress, int connectTimeout,
//  int readTimeout, List<Class<? extends py4j.commands.Command>> customCommands) {
  InetAddress javaPythonAddr = InetAddress.getByName("0.0.0.0");
  InetAddress pythonJavaAddr = InetAddress.getByName("0.0.0.0");

  int timeout = 30000;  // 30 seconds

  GatewayServer gateway = new GatewayServer(null, javaPythonPort, pythonJavaPort,
      javaPythonAddr, pythonJavaAddr, timeout, 0, null);
  gateway.start();

  int listeningPort = gateway.getListeningPort();
  if (listeningPort == -1 || listeningPort != javaPythonPort)
  {
    throw new IOException("GatewayServer failed to bind");
  }

  log.info("Starting PythonGatewayServer. Communicating (java->python) on port " + listeningPort);

  sendGatewayPort(clientSocket, javaPythonPort, pythonJavaPort);

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
        int port = server.getListeningPort();

        // Exit on EOF or broken pipe to ensure that this process dies when the Python driver dies:
        try
        {
          Semaphore semaphore = new Semaphore(1);
          server.addListener(new ServerListener(semaphore));

          // Wait for the shutdown
          semaphore.acquire();
        }
        catch (InterruptedException ignored)
        {
        }

        if (portQueue != null)
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

private void sendGatewayPort(Socket clientSocket, int javaPythonPort, int pythonJavaPort) throws IOException
{
  // Communicate the bound port back to the caller via the caller-specified callback port
  log.info("Sending java->python port (" + javaPythonPort + ") and python->java port (" + pythonJavaPort +
      ") to pymrgeo running at " +
      clientSocket.getInetAddress().getHostName() +"(" + clientSocket.getInetAddress().getHostAddress() +
      ")" + ":" + clientSocket.getPort());

  try
  {
    DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
    dos.writeInt(javaPythonPort);
    dos.writeInt(pythonJavaPort);
    dos.close();
  }
  catch (IOException e)
  {
    throw new IOException("Can not write to socket callback socket", e);
  }
}

}
