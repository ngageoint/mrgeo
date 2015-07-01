/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

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
