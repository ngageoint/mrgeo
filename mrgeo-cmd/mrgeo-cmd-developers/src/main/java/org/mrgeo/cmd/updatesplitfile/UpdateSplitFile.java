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

package org.mrgeo.cmd.updatesplitfile;

import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.mrgeo.cmd.Command;
import org.mrgeo.hdfs.tile.SplitFile;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;

/**
 * A utility class to print split files on the command line
 * 
 * PrintSplitFile <split filename>
 **/

public class UpdateSplitFile extends Command
{

  @Override
  public int run(final String[] args, final Configuration conf,
      final Properties providerProperties)
  {
    try
    {
      final CommandLineParser parser = new PosixParser();
      final CommandLine line = parser.parse(new Options(), args);

      final String splitFile = line.getArgs()[0];
      final FileSystem fs = HadoopFileUtils.getFileSystem(conf);

      final Path splitFilePath = new Path(splitFile);

      final Path tmp = new Path(HadoopFileUtils.getTempDir(), HadoopUtils.createRandomString(4));

      FileUtil.copy(fs, splitFilePath, fs, tmp, false, true, conf);

      final SplitFile sf = new SplitFile(conf);
      sf.copySplitFile(tmp.toString(), splitFilePath.getParent().toString(), true);

      return 0;
    }
    catch (final Exception e)
    {
      e.printStackTrace();
    }

    return -1;
  }
}