package org.mrgeo.cmd.vectorinfo;

import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.CommandSpi;

public class VectorInfoSpi extends CommandSpi
{

  @Override
  public Class<? extends Command> getCommandClass()
  {
    return VectorInfo.class;
  }

  @Override
  public String getCommandName()
  {
    return "vectorinfo";
  }

  @Override
  public String getDescription()
  {
    return "Show information on a vector data source (developer cmd)";
  }

}
