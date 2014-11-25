package org.mrgeo.cmd.ingestvector;

import org.mrgeo.cmd.Command;
import org.mrgeo.cmd.CommandSpi;


public class IngestVectorSpi extends CommandSpi
{

  @Override
  public Class<? extends Command> getCommandClass()
  {
    return IngestVector.class;
  }

  @Override
  public String getCommandName()
  {
    return "ingestvector";
  }

  @Override
  public String getDescription()
  {
    return "Ingest a vector source into a MrsVector";
  }
  
}
