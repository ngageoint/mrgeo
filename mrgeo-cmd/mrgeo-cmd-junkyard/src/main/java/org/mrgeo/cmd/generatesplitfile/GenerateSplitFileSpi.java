package org.mrgeo.cmd.generatesplitfile;

import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.CommandSpi;


public class GenerateSplitFileSpi extends CommandSpi
{

  @Override
  public Class<? extends Command> getCommandClass()
  {
    return GenerateSplitFile.class;
  }

  @Override
  public String getCommandName()
  {
    return "generatesplitfile";
  }

  @Override
  public String getDescription()
  {
    return "Generate a splits file (developer cmd)";
  }
  
}
