/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.pig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PigQuerier
{
  private static final Logger log = LoggerFactory.getLogger(PigQuerier.class);

  public PigQuerier()
  {
    
  }
  
  public void query(String str, Path output, Configuration conf) throws IOException
  {
    log.info("query: " + str);
    log.info("output: " + output.toString());
    ExecType et = ExecType.MAPREDUCE;
    if (conf.get("mapred.job.tracker").equals("local"))
    {
      et = ExecType.LOCAL;
    }

    PigServer ps = new PigServer(et);

    ps.registerJar(HadoopUtils.getJar(conf, this.getClass()));
    ps.registerFunction("MrGeoLoader", new FuncSpec("org.mrgeo.pig.AutoLoadFunc"));
    ps.registerFunction("MrGeoCsvStorage", new FuncSpec("org.mrgeo.pig.AutoLoadFunc"));
    
    String[] lines = str.split(";");
    for (String l : lines)
    {
      l = l.replace("\n", " ");
      l = l.trim();
      if (l.length() > 0)
      {
        l = l + ";";
        log.info("Parsing: {}\n", l);
        ps.registerQuery(l);
      }
    }
    ps.store("result", output.toString(), "org.mrgeo.pig.CsvStoreFunc");
    
    ps.shutdown();
  }
}
