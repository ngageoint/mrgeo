package org.mrgeo.cmd.upgradepyramid;

import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.CommandSpi;


public class UpgradePyramidSpi extends CommandSpi
{

  @Override
  public Class<? extends Command> getCommandClass()
  {
    return UpgradePyramid.class;
  }

  @Override
  public String getCommandName()
  {
    return "upgradepyramid";
  }

  @Override
  public String getDescription()
  {
    return "Upgrade a MrsPyramid from v1 to v2 (developer cmd)";
  }
  
}
