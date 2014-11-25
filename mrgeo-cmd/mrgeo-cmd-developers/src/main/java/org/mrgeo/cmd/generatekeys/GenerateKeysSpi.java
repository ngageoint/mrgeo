package org.mrgeo.cmd.generatekeys;

import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.CommandSpi;


public class GenerateKeysSpi extends CommandSpi
{

  @Override
  public Class<? extends Command> getCommandClass()
  {
    return GenerateKeys.class;
  }

  @Override
  public String getCommandName()
  {
    return "generatekeys";
  }

  @Override
  public String getDescription()
  {
    return "Generate keys (developer cmd)";
  }
  
}
