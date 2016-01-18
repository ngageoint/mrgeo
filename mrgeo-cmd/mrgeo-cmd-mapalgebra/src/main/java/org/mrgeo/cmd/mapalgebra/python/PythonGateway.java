package org.mrgeo.cmd.mapalgebra.python;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.MrGeo;
import org.mrgeo.data.ProviderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayServer;

import java.io.DataOutputStream;
import java.io.IOException;
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

  Option port = new Option("p", "port", true, "Callback port");
  port.setRequired(false);
  result.addOption(port);

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

    // Start a GatewayServer on an ephemeral port
    GatewayServer gateway = new GatewayServer(null, 0);
    gateway.start();

    int port = gateway.getListeningPort();
    if (port == -1)
    {
      log.error("GatewayServer failed to bind; exiting");
      System.exit(-1);
    }

    log.info("Started PythonGatewayServer on port " + port);


//  // Communicate the bound port back to the caller via the caller-specified callback port
    String callbackHost = line.getOptionValue("h");
    int callbackPort = Integer.parseInt(line.getOptionValue("p"));

    log.info("Sending GatewayServer port to python driver at " + callbackHost  + ":" + callbackPort);

    try (Socket callbackSocket = new Socket(callbackHost, callbackPort))
    {
      DataOutputStream dos = new DataOutputStream(callbackSocket.getOutputStream());
      dos.writeInt(port);
      dos.close();
    }
    catch (IOException e)
    {
      log.error("Can not establish callback socket");
    }

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

    log.error("Exiting due to broken pipe from Python driver");
    return 0;

  }
  catch (ParseException e)
  {
    e.printStackTrace();
    return -1;
  }
}

}
