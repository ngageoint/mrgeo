package org.mrgeo.rasterops;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CostDistanceWorkersConfiguration
{
  private static final Logger LOG = LoggerFactory.getLogger(CostDistanceWorkersConfiguration.class);

  /*
   * The fudge factor controls the mbPerTile computation and is used to account for memory overhead 
   * on top of the 3-band raster that is at the heart of the in-memory state maintained by CostDistance. 
   * There are two types of this extra overhead:
   * 1. Giraph messages and edges 
   * 2. Account for unbalanced splits to workers assignment
   * #2 is especially tricky to account for. The problem is that while we can account for the 
   * number of tiles in the AOI spanned by the problem, we can't account for the number of 
   * splits that each worker will eventually be assigned and the number of tiles per split, 
   * since splits calculation is done in the input format which is run after the number-of-workers
   * calculation in CostDistanceWorkersConfiguration. So we use a fudge factor to inflate the 
   * number-of-tiles-per-worker calculation
   */
  private final static float FUDGE_FACTOR = 2.2f;

  // higher means each worker gets fewer tiles
  private final static int TILE_PER_WORKER_MULTIPLIER = 8;

  public static int getNumWorkers(MrsImagePyramidMetadata metadata, Configuration conf)
      throws NumberFormatException, IOException
  {
    return getNumWorkers(metadata, conf, true);
  }

  public static int getNumWorkers(MrsImagePyramidMetadata metadata, Configuration conf,
      boolean checkClusterCapacity) throws NumberFormatException, IOException
  {
    final Properties properties = MrGeoProperties.getInstance();
    if (properties.containsKey("giraph.workers"))
      return Integer.valueOf(properties.getProperty("giraph.workers"));

    TiledInputFormatContext ifContext = TiledInputFormatContext.load(conf);
    // initialize the bounds
    Bounds bounds = Bounds.convertOldToNewBounds(ifContext.getBounds());
    if (bounds == null)
    {
      throw new IllegalStateException("Unable to extract bounds out of configuration");
    }

    int zoomLevel = ifContext.getZoomLevel();
    // TODO - hard-coded to use only first source point!
    TMSUtils.TileBounds tb = TMSUtils.boundsToTile(bounds, zoomLevel, metadata.getTilesize());

    // add 1 since boundsToTile gives back tiles inclusive
    long numTiles = (tb.e - tb.w + 1) * (tb.n - tb.s + 1);

    /*
    The algorithm is:
    1. Get the heap size of each worker 
      - I need to find out a good way to estimate this, but for now, use 
        (mapred.child.java.opts - io.sort.mb) as an approximation
    2. Estimate mbPerTile (see getMbPerTile)
    3. Divide ResultsFrom1 / mbPerTile. This gives us number of tiles per worker
    4. Divide numTiles / ResultsFrom3. Round this up. This gives us number of workers. 
    5. Do basic error checking. If ResultsFrom3 > NumberMapSlotsInCluster, throw error and exit
    */

    // get map slots
    final int numMapSlots = getMapSlots(conf);

    // get heap per slot
    final int heapPerSlot = getHeapPerSlot(conf);

    // get sort mb
    final int sortMb = conf.getInt("io.sort.mb", -1);
    if (sortMb == -1)
    {
      throw new IllegalStateException("Configuration is missing \"io.sort.mb\" - cannot estimate"
          + " java heap size of each worker without it");
    }

    final int effHeapPerSlot = heapPerSlot - sortMb;
    final int mbPerTile = getMbPerTile(metadata);
    final int numTilesPerWorker = effHeapPerSlot / mbPerTile;
    if (numTilesPerWorker <= 0)
    {
      throw new IllegalStateException("Invalid value for numTilesPerWorker - " + numTilesPerWorker);
    }

    final int numWorkers = Math.max((int) Math.ceil((double) numTiles / numTilesPerWorker), 1);
    LOG.info(String.format(
        "CostDistanceConfiguration: mapSlots=%d, effheapPerSlot=%d, mbPerTile=%d, numTiles=%d, "
            + "numTilesPerWorker=%d, numWorkers=%d, tileBounds=%s", numMapSlots, effHeapPerSlot,
        mbPerTile, numTiles, numTilesPerWorker, numWorkers, tb));

    // TODO: we can't really check the slots available in YARN, there is no
    // longer a
    // concept of slots...
    if (checkClusterCapacity && numMapSlots >= 0 && numWorkers > numMapSlots - 1)
    {
      throw new IllegalArgumentException(String.format(
          "CostDistance needs at least %d map slots on the cluster "
              + "which currently has only %d map slots. Either increase "
              + "the amount of RAM per map task using mapred.child.java.opts "
              + "or increase the number of map slots to at least %d.", (numWorkers + 1),
          numMapSlots, (numWorkers + 1)));
    }
    return numWorkers;
  }

  public static int getHeapPerSlot(Configuration conf)
  {
    final String childJavaOpts = conf.get("mapred.child.java.opts", "");

    if (childJavaOpts == "")
    {
      throw new IllegalArgumentException("Illegal configuration - does not have "
          + "\"mapred.child.java.opts\" defined");
    }
    final String[] childJavaOptsSplit = childJavaOpts.split(" ");

    int heapPerSlot = -1;
    for (String childJavaOpt : childJavaOptsSplit)
    {
      if (childJavaOpt.contains("Xmx"))
      {
        Pattern p = Pattern.compile("-Xmx([0-9]+)([a-zA-Z])");
        Matcher m = p.matcher(childJavaOpt);
        if (!m.matches())
        {
          throw new IllegalStateException("Invalid syntax for -Xmx in configuration - value of "
              + "\"mapred.child.java.opts\" is " + childJavaOpt);
        }

        final int numberPart = Integer.valueOf(m.group(1));
        final String unitPart = m.group(2);

        if (unitPart.equalsIgnoreCase("G"))
          heapPerSlot = numberPart * 1024;
        else if (unitPart.equalsIgnoreCase("M"))
          heapPerSlot = numberPart;
        else if (unitPart.equalsIgnoreCase("K"))
          heapPerSlot = numberPart / 1024;
      }
    }

    if (heapPerSlot == -1)
      throw new IllegalStateException("Unable to compute heapPerSlot");

    return heapPerSlot;
  }

  private static int getMbPerTile(MrsImagePyramidMetadata metadata)
  {
    final int tileSize = metadata.getTilesize();
    final int bytesPerPixel = MrsImagePyramidMetadata.toBytes(metadata.getTileType());
    final int bytesPerTile = tileSize * tileSize * PixelSizeMultiplier.NUM_BANDS * bytesPerPixel;
    final int mbPerTile = (int) Math.round(((float) bytesPerTile * FUDGE_FACTOR) / (1024 * 1024));
    return mbPerTile;
  }

  private static int getMapSlots(Configuration conf) throws NumberFormatException, IOException
  {
    String yarn = conf.get("mapreduce.framework.name", "");
    JobClient client;
    if (!yarn.contains("yarn"))
    {
      String jobTracker = conf.get("mapred.job.tracker", null);
      if (jobTracker != null && jobTracker.indexOf(":") > 0)
      {
        String jobTrackerSplit[] = jobTracker.split(":");
        client = new JobClient(new InetSocketAddress(jobTrackerSplit[0],
            Integer.valueOf(jobTrackerSplit[1])), conf);
      }
      else
      {
        JobConf jc = new JobConf(conf);
        client = new JobClient(jc);
      }
    }
    else
    {
      // no slots in yarn...
      return -1;
    }

    final int mapSlots = client.getClusterStatus().getMaxMapTasks();
    client.close();

    return mapSlots;
  }
}
