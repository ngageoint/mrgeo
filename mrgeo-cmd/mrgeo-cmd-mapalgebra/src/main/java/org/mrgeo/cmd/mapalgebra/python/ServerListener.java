package org.mrgeo.cmd.mapalgebra.python;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayConnection;
import py4j.GatewayServerListener;

import java.net.Socket;
import java.util.concurrent.Semaphore;


public class ServerListener implements GatewayServerListener
{
private static final Logger log = LoggerFactory.getLogger(ServerListener.class);

private Semaphore semaphore;

ServerListener(Semaphore semaphore) throws InterruptedException
{
  this.semaphore = semaphore;

  semaphore.acquire();
}

@Override
public void connectionError(Exception e)
{
log.warn("Connection error");
}

@Override
public void connectionStarted(GatewayConnection gatewayConnection)
{
  Socket socket = gatewayConnection.getSocket();

  log.warn("Started connection " +
      socket.getInetAddress().getHostName() +"(" + socket.getInetAddress().getHostAddress() +
      ")" + ":" + socket.getLocalPort());
}

@Override
public void connectionStopped(GatewayConnection gatewayConnection)
{
  Socket socket = gatewayConnection.getSocket();

  log.warn("Stopped connection " +
      socket.getInetAddress().getHostName() +"(" + socket.getInetAddress().getHostAddress() +
      ")" + ":" + socket.getLocalPort());

}

@Override
public void serverError(Exception e)
{
  log.warn("Server error");
  e.printStackTrace();
}

@Override
public void serverPostShutdown()
{
  log.warn("Server Post Shutdown");

  semaphore.release();
}

@Override
public void serverPreShutdown()
{
  log.warn("Server Pre Shutdown");

}

@Override
public void serverStarted()
{
  log.warn("Server started");

}

@Override
public void serverStopped()
{
  log.warn("Server stopped");

}
}
