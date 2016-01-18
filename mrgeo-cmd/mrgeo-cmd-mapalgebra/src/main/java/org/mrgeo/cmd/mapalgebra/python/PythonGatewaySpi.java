package org.mrgeo.cmd.mapalgebra.python;

import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.CommandSpi;

public class PythonGatewaySpi extends CommandSpi
{

@Override
public Class<? extends Command> getCommandClass()
{
  return PythonGateway.class;
}

@Override
public String getCommandName()
{
  return "python";
}

@Override
public String getDescription()
{
  return "Start the gateway server for communicating with python";
}

}

