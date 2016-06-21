package org.mrgeo.cmd.mapalgebra.python;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.GatewayConnection;
import py4j.GatewayServerListener;

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
  log.warn("Connection started");

}

@Override
public void connectionStopped(GatewayConnection gatewayConnection)
{
  log.warn("Connection stopped");

}

@Override
public void serverError(Exception e)
{
  log.warn("Server error");

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
