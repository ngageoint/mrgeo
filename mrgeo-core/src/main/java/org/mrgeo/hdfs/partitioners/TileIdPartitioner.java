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
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Partitioner;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.tile.SplitFile;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.tile.SplitGenerator;
import org.mrgeo.tile.TileIdZoomWritable;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  private static final String SPLITFILE_KEY = PREFIX + ".splitFile.";

  private static final String SPLITFILE_USE_DISTRIBUTED_CACHE = PREFIX + ".useDistributedCache";
  public static final String INCREMENT_KEY = PREFIX + ".increment";
  public static final String MAX_PARTITIONS_KEY = PREFIX + ".maxPartitions";

  // zoom-level entry to use for the single split point case
  private static final int SINGLE_SPLIT = 0;

  // split file has partition names

  private Configuration conf;
  private final Map<Integer, List<SplitFile.SplitInfo>> splitPoints = new HashMap<Integer, List<SplitFile.SplitInfo>>();

  // If we're using a multiple split point case (not SINGLE_SPLIT), we need to offset each zoom
  // level's
  // partitions based on the total number of partitions in all the higher (lower numbered) zoom
  // levels
  private final Map<Integer, Integer> splitPointOffsets = new HashMap<Integer, Integer>();

  /**
   * @param file
   *          - local/hdfs path to split file
   * @param conf
   * 
   *          <p>
   *          Set split file path.
   * 
   */
  public static void setSplitFile(final String file, final Configuration conf)
  {
    setSplitFile(file, conf, SINGLE_SPLIT);
  }

  public static void setSplitFile(final String file, final Configuration conf, final int zoom)
  {
    conf.set(SPLITFILE_KEY + zoom, file);
    log.debug("Adding: [" + file + "] to conf: " + (SPLITFILE_KEY + zoom));
  }

  /**
   * @param file
   *          - local/hdfs path to split file
   * @param job
   * 
   *          <p>
   *          Set split file path, add file to distributed cache, and set useDistributedCache flag
   *          to true
   */
  public static void setSplitFile(final String file, final JobContext job)
  {
    setSplitFile(file, job, SINGLE_SPLIT);
  }

  /**
   * @param file
   *          - local/hdfs path to split file
   * @param job
   * 
   *          <p>
   *          Set split file path, add file to distributed cache, and set useDistributedCache flag
   *          to true
   */
  public static void setSplitFile(final String file, final JobContext job, final int zoom)
  {
    final Configuration conf = job.getConfiguration();
    setSplitFile(file, conf, zoom);
    final URI uri = new Path(file).toUri();
    DistributedCache.addCacheFile(uri, conf);
    log.debug("Adding: " + uri.toString() + " to Distributed cache");
    conf.setBoolean(SPLITFILE_USE_DISTRIBUTED_CACHE, true);
  }

  /**
   * @param job
   * @param splitFile
   * @return
   * @throws IOException
   * 
   *           <p>
   *           Sets up the partitioner and the number of reduce tasks based on the number of splits.
   *           Caller should not also set the number of reduce tasks - bad things can happen. If
   *           splitFile is null, then a new one is created in hadoop's tmp directory, and its path
   *           is returned. If it is not null, it is used by the partitioner, and also returned.
   * 
   *           See copySplitFile() also.
   */
  public static Path setup(final Job job, final Path splitFile) throws IOException
  {
    return setup(job, null, splitFile, SINGLE_SPLIT);
  }

  public static Path setup(final Job job, final Path splitFile, final int zoom) throws IOException
  {
    return setup(job, null, splitFile, zoom);
  }

  /**
   * @param job
   * @param splitGenerator
   * @return path to splits.txt in hadoop's tmp directory.
   * @throws IOException
   * 
   *           <p>
   *           Sets up the partitioner and the number of reduce tasks based on the number of splits.
   *           Caller should not also set the number of reduce tasks - bad things can happen. As
   *           part of the setup process, a "splits" file is created in hadoop's tmp directory.
   * 
   *           See copySplitFile() also.
   */
  public static Path setup(final Job job, final SplitGenerator splitGenerator) throws IOException
  {
    return setup(job, splitGenerator, null, SINGLE_SPLIT);
  }

  public static Path setup(final Job job, final SplitGenerator splitGenerator, final int zoom)
      throws IOException
      {
    return setup(job, splitGenerator, null, zoom);
      }

  private synchronized static List<SplitFile.SplitInfo> getSplitPointsFromDistributedCache(
    final String splitFileName, final Configuration config) throws IOException
    {
    final Path[] cf = DistributedCache.getLocalCacheFiles(config);
    List<SplitFile.SplitInfo> splitPoints = null;
    if (cf != null)
    {
      SplitFile sf = new SplitFile(config);
      for (final Path path : cf)
      {
        if (path.toUri().getPath()
            .endsWith(splitFileName.substring(splitFileName.lastIndexOf('/'))))
        {
          final String modifiedPath = "file://" + path.toString();
          splitPoints = sf.readSplitsCurrentVersion(modifiedPath);
          break;
        }
      }
    }
    return splitPoints;
    }

  private static Path setup(final Job job, final SplitGenerator splitGenerator, Path splitFile,
    final int zoom) throws IOException
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

    int numSplits;
    SplitFile sf = new SplitFile(job.getConfiguration());
    if (splitFile == null)
    {
      // create a split file in the hadoop tmp directory
      // this is copied into the job's output directory upon job completion
      final int uniquePrefixLen = 5;
      splitFile = new Path(HadoopFileUtils.getTempDir(job.getConfiguration()),
          HadoopUtils.createRandomString(uniquePrefixLen) +
        "_" + SplitFile.SPLIT_FILE);

      assert (splitGenerator != null);
      numSplits = sf.writeSplits(splitGenerator, splitFile.toString());
    }
    else
    {
      numSplits = sf.numSplitsCurrentVersion(splitFile.toString());
    }

    int original = job.getNumReduceTasks();
    
    // remember, the number of partitions (reducers) is one more than the splits
//    job.setNumReduceTasks(original + (numSplits > 0 ? numSplits + 1 : 1));
    job.setNumReduceTasks(numSplits);

    log.debug("Increasing reduce tasks from: " + original + " to: " + job.getNumReduceTasks());

    job.setPartitionerClass(TileIdPartitioner.class);

    TileIdPartitioner.setSplitFile(splitFile.toString(), job, zoom);
    return splitFile;
    }

  @Override
  public Configuration getConf()
  {
    return conf;
  }

  @Override
  public int getPartition(final KEY key, final VALUE value, final int numPartitions)
  {
    try
    {
      if (key instanceof TileIdZoomWritable)
      {
        final int zoom = ((TileIdZoomWritable) key).getZoom();

        return SplitFile.findPartition(((TileIdWritable) key).get(), getSplitPoints(zoom)) +
            splitPointOffsets.get(zoom);
      }
      if (key instanceof TileIdWritable)
      {
        return SplitFile.findPartition(((TileIdWritable) key).get(), getSplitPoints());
      }
    }
    catch (final IOException e)
    {
      throw new RuntimeException(e);
    }

    throw new RuntimeException("Bad type sent into TileIdPartitioner.getPartition(): " +
        key.getClass());
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


  @Override
  public void setConf(final Configuration conf)
  {
    this.conf = conf;
  }

  private List<SplitFile.SplitInfo> getSplitPoints() throws IOException
  {
    return getSplitPoints(SINGLE_SPLIT);
  }

  private synchronized List<SplitFile.SplitInfo> getSplitPoints(final int zoom) throws IOException
  {
    SplitFile sf = new SplitFile(conf);
    if (!splitPoints.containsKey(zoom))
    {
      // to calculate the offsets, we'll need to calculate all the zoom levels up to the one we're
      // interested in...
      for (int i = 0; i <= zoom; i++)
      {
        if (!splitPoints.containsKey(i))
        {
          List<SplitFile.SplitInfo> splitPoints = null;

          final String splitFileName = conf.get(SPLITFILE_KEY + i, null);
          if (splitFileName != null)
          {
            final boolean useDistributedCache = conf.getBoolean(SPLITFILE_USE_DISTRIBUTED_CACHE,
              false);
            if (useDistributedCache)
            {
              splitPoints = getSplitPointsFromDistributedCache(splitFileName, conf);
            }

            // if we don't have a cache, or we can't find the splits in the cache, try a normal
            // read.
            if (splitPoints == null)
            {
              splitPoints = sf.readSplitsCurrentVersion(splitFileName);
            }

            if (splitPoints == null)
            {
              throw new FileNotFoundException(" Unable to read split file \"" + splitFileName +
                "\". useDistributedCache = " + useDistributedCache);
            }

            this.splitPoints.put(i, splitPoints);
          }
        }
      }
      int offset = 0;
      for (int i = 0; i <= zoom; i++)
      {
        if (splitPoints.containsKey(i))
        {
          splitPointOffsets.put(i, offset);

          final int len = splitPoints.get(i).size();

          offset += len > 0 ? len : 1;
        }
      }
    }
    return splitPoints.get(zoom);

  }

}
