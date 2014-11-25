package org.mrgeo.cmd.updatesplitfile;

import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.CommandSpi;


public class UpdateSplitFileSpi extends CommandSpi
{

  @Override
  public Class<? extends Command> getCommandClass()
  {
    return UpdateSplitFile.class;
  }

  @Override
  public String getCommandName()
  {
    return "updatesplits";
  }

  @Override
  public String getDescription()
  {
    return "Update the splits file (developer cmd)";
  }
  
}
