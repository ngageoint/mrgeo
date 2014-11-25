package org.mrgeo.cmd.printsplitfile;

import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.CommandSpi;


public class PrintSplitFileSpi extends CommandSpi
{

  @Override
  public Class<? extends Command> getCommandClass()
  {
    return PrintSplitFile.class;
  }

  @Override
  public String getCommandName()
  {
    return "printsplit";
  }

  @Override
  public String getDescription()
  {
    return "Print the splits file (developer cmd)";
  }
  
}
