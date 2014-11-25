package org.mrgeo.cmd.showconfiguration;

import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.CommandSpi;


public class ShowConfigurationSpi extends CommandSpi
{

  @Override
  public Class<? extends Command> getCommandClass()
  {
    return ShowConfiguration.class;
  }

  @Override
  public String getCommandName()
  {
    return "showconf";
  }

  @Override
  public String getDescription()
  {
    return "Show configuration (developer cmd)";
  }
  
}
