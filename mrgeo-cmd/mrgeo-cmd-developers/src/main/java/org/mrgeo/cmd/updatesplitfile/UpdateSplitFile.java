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