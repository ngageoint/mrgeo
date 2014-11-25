package org.mrgeo.cmd.mrsimageinfo;

import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.CommandSpi;


public class MrsImageInfoSpi extends CommandSpi
{

  @Override
  public Class<? extends Command> getCommandClass()
  {
    return MrsImageInfo.class;
  }

  @Override
  public String getCommandName()
  {
    return "info";
  }

  @Override
  public String getDescription()
  {
    return "Show information on a MrsImage (developer cmd)";
  }
  
}
