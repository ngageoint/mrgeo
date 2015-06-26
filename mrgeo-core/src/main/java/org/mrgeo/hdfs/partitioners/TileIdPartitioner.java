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

package org.mrgeo.hdfs.partitioners;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.tile.PartitionerSplit;
import org.mrgeo.hdfs.tile.Splits;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * The TileIdPartitioner works by reading split points from a file and producing partition numbers
 * based on these split points. This split file is usually resident in the distributed cache, from
 * where it can be read by map tasks for properly partitioning tiles amongst reducers.
 *
 * There is also a use case for reading it from the file system - hdfs or local for non-map/reduce
 * clients (e.g., MrsImageReader). These clients use setSplitFile to point a configuration variable
 * to a file in the local/hdfs file system.
 *
 * If callers are using TileIdPartitioner in the map/reduce job, they should rely on
 * TileIdPartitioner to set the number of reduce tasks. Doing so themselves can cause inconsistent
 * results. See documentation for setup() below.
 *
 */
public class TileIdPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE> implements Configurable
{
  private static final Logger log = LoggerFactory.getLogger(TileIdPartitioner.class);
  private static final String PREFIX = TileIdPartitioner.class.getSimpleName();
  private static final String SPLITFILE_KEY = PREFIX + ".splitFile";
  private static final String SPLITFILE_USE_DISTRIBUTED_CACHE = PREFIX + ".useDistributedCache";

  public static final String INCREMENT_KEY = PREFIX + ".increment";
  public static final String MAX_PARTITIONS_KEY = PREFIX + ".maxPartitions";

  private Configuration conf = null;
  private Splits splits = null;

  /**
   * @param file
   *          - local/hdfs path to split file
   * @param job
   *          - the Hadoop job
   *
   *          <p>
   *          Set split file path.
   *
   */
  public static void setSplitFile(final String file, final Job job)
  {
    Configuration conf = job.getConfiguration();

    conf.set(SPLITFILE_KEY, file);
    log.debug("Adding \"" + SPLITFILE_KEY + " = " + file + "\" to configuration");

    if (!HadoopUtils.isLocal(conf))
    {
      final URI uri = new Path(file).toUri();
      job.addCacheFile(uri);

      log.debug("Adding: \"" + uri.toString() + "\" to Distributed cache");
      conf.setBoolean(SPLITFILE_USE_DISTRIBUTED_CACHE, true);
    }
  }


  private static Path setup(final Job job, final SplitGenerator splitGenerator) throws IOException
  {
    // don't set up a partitioner in local mode
    if (HadoopUtils.isLocal(job.getConfiguration()))
    {
      // make sure we have at least 1 reducer...
      if (job.getNumReduceTasks() < 1)
      {
        job.setNumReduceTasks(1);
      }
      return null;
    }

    PartitionerSplit splits = new PartitionerSplit();

    splits.generateSplits(splitGenerator);
    
    // create a split file in the hadoop tmp directory
    // this is copied into the job's output directory upon job completion
    final int uniquePrefixLen = 5;
    Path splitFile = new Path(HadoopFileUtils.getTempDir(job.getConfiguration()),
        HadoopUtils.createRandomString(uniquePrefixLen) +
            "_" + PartitionerSplit.SPLIT_FILE);

    splits.writeSplits(splitFile);

    job.setNumReduceTasks(splits.length());
    job.setPartitionerClass(TileIdPartitioner.class);

    setSplitFile(splitFile.toString(), job);

    return splitFile;
  }

  @Override
  public void setConf(final Configuration conf)
  {
    this.conf = conf;
  }

  @Override
  public Configuration getConf()
  {
    return conf;
  }

  @Override
  public int getPartition(final KEY key, final VALUE value, final int numPartitions)
  {

    if (key instanceof TileIdWritable)
    {
      try
      {
        if (splits == null)
        {
          loadSplits();
        }

        return splits.getSplit(((TileIdWritable) key).get()).getPartition();
      }
      catch (Exception e)
      {
        throw new RuntimeException("Error getting partition", e);
      }

    }

    throw new RuntimeException("Bad type sent into TileIdPartitioner.getPartition(): " +
        key.getClass());
  }

  private void loadSplits() throws IOException
  {
    if (conf == null)
    {
      throw new RuntimeException("Configuration has not been set in TileIdPartitioner");
    }

    String file = conf.get(SPLITFILE_KEY);
    if (conf.getBoolean(SPLITFILE_USE_DISTRIBUTED_CACHE, false))
    {
      try
      {
        final Path[] cf = DistributedCache.getLocalCacheFiles(conf);
        for (final Path path : cf)
        {
          if (path.toUri().getPath()
              .endsWith(file.substring(file.lastIndexOf('/'))))
          {
            final String modified = "file://" + path.toString();

            splits = new PartitionerSplit();
            splits.readSplits(new Path(modified));
          }
        }
      }
      catch (IOException e)
      {
        throw new IOException("Error trying to read splits from distributed cache: " + file, e);
      }
    }
    else
    {
      splits = new PartitionerSplit();
      splits.readSplits(new Path(file));
    }
  }

  @SuppressWarnings("unchecked")
  public int getPartition(final TileIdWritable key)
  {
    return getPartition((KEY) key, null, 0);
  }

  @SuppressWarnings("unchecked")
  public int getPartition(final TileIdWritable key, final VALUE value, final int size)
  {
    return getPartition((KEY) key, value, size);
  }



}
