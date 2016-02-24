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
import java.net.ServerSocket;
import java.net.Socket;

public class PythonGateway extends Command
{
private static final Logger log = LoggerFactory.getLogger(PythonGateway.class);

public static Options createOptions()
{
  Options result = MrGeo.createOptions();

  Option host = new Option("h", "host", true, "Callback hostname");
  host.setRequired(false);
  result.addOption(host);

  Option port = new Option("p", "port", true, "Callback or listen port");
  port.setRequired(false);
  result.addOption(port);

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
    setupSingleServer(callbackHost, callbackPort);

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
  // Start a GatewayServer on an ephemeral port
  GatewayServer gateway = new GatewayServer(null, 0);
  gateway.start();

  int port = gateway.getListeningPort();
  if (port == -1)
  {
    throw new IOException("GatewayServer failed to bind");
  }

  log.info("Starting PythonGatewayServer on port " + port);

  sendGatewayPort(callbackHost, callbackPort, port);

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
        setupSingleServer(callbackHost, callbackPort);

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

private void sendGatewayPort(String callbackHost, int callbackPort, int port) throws IOException
{
  // Communicate the bound port back to the caller via the caller-specified callback port
  log.info("Sending GatewayServer port to python driver at " + callbackHost  + ":" + callbackPort);

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
