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

package org.mrgeo.data;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.mapreduce.formats.TileCollection;
import org.mrgeo.mapreduce.splitters.MrsPyramidInputSplit;
import org.mrgeo.mapreduce.splitters.TiledInputSplit;
import org.mrgeo.pyramid.MrsPyramid;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * This class is the base class for the Hadoop InputFormat classes that are configured
 * into Hadoop jobs submitted by MrGeo for processing pyramid data. Map/reduce jobs
 * on pyramid input data allow multiple inputs. In order to make that work, this class
 * ensures that each split contains the bounds of the splits that are ordered before
 * them and the bounds of the splits that come after them. This way, the RecordReader
 * classes for pyramid data can use that information to ensure that tiles are only
 * ever read once, regardless of which input pyramids contain data in that tile.
 */
public abstract class MrsPyramidInputFormat<V> extends InputFormat<TileIdWritable, TileCollection<V>>
{
  static Logger log = LoggerFactory.getLogger(MrsPyramidInputFormat.class);

  public MrsPyramidInputFormat()
  {
  }

  /**
   * Sub-classes must override this method so that the data access layer being used can
   * return the native splits for that specific data format.
   * 
   * @param context
   * @param ifContext
   * @param input
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  protected abstract List<TiledInputSplit> getNativeSplits(final JobContext context,
      final TiledInputFormatContext ifContext, final String input) throws IOException, InterruptedException;

  /**
   * Returns the list of MrsPyramidInputSplit objects required across all of the
   * input pyramids. Sub-classes should have no need to override this method. It
   * contains logic required by all input formats (described in the overview
   * for this class).
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    // Get the TiledInputFormatContext from the JobContext
    TiledInputFormatContext ifContext = TiledInputFormatContext.load(context.getConfiguration());

    // Get the list of inputs
    Set<String> inputSet = ifContext.getInputs();
//    String[] inputs = new String[inputSet.size()];
//    int inputIndex = 0;
//    for (String strInput : inputSet)
//    {
//      inputs[inputIndex++] = strInput;
//    }

    // only 1 file?, no more processing needed
//    if (inputs.length == 1)
//    {
//        return nativeSplits;
//    }

    MrsPyramid[] pyramids = new MrsPyramid[inputSet.size()];
    // Collect the pyramids
//  for (int i=0; i < inputs.length; i++)
    int inputIndex = 0;
    for (String strInput : inputSet)
    {
      // The path is to the actual pyramid, so we need to get the parent to get the pyramid. Yuck!
      String pyramid = strInput;
      MrsPyramid p = MrsImagePyramid.open(pyramid, context.getConfiguration());
      pyramids[inputIndex] = p;
      inputIndex++;
    }

    // TODO: Consider sorting based on the number of tiles for each source
    // so that we sort in descending order by number of tiles. This way we
    // split over more tiles which should distribute the work across more
    // mappers.
    // sort from largest to smallest
    Arrays.sort(pyramids, new Comparator<MrsPyramid>(){
      @Override
      public int compare(MrsPyramid p1, MrsPyramid p2) {
        Bounds b = p1.getBounds();
        double a1 = b.getWidth() * b.getHeight();

        b = p2.getBounds();
        double a2 = b.getWidth() * b.getHeight();

        // make this Double.compare(a2, a1) for largest to smallest sort
        int result = Double.compare(a2, a1);
        // If the pyramids are the same size, then sort the higher res pyramids
        // above the lower res so that we map/reduce over the pyramid with
        // the most tiles for map/reduce efficiency.
        if (result == 0)
        {
          int v1 = p1.getMaximumLevel();
          int v2 = p2.getMaximumLevel();
          if (v1 == v2)
          {
            return 0;
          }
          return (v1 < v2) ? 1 : -1;
        }
        return result;
      }
    });

    List<List<TiledInputSplit>> nativeSplitsPerInput = new LinkedList<List<TiledInputSplit>>();

    // For each input, get its native input splits, zoom, and load its MrsImagePyramid.
    Map<String, Bounds> post = new HashMap<String, Bounds>();
    int[] zooms = new int[pyramids.length];
    for (int i=0; i < pyramids.length; i++)
    {
      String pyramid = pyramids[i].getName();
      zooms[i] = ifContext.getZoomLevel();
      List<TiledInputSplit> splits = getNativeSplits(context, ifContext, pyramid);
      log.info("Native split count: " + splits.size());
      List<TiledInputSplit> filteredSplits = filterInputSplits(ifContext,
              splits, zooms[i],
              pyramids[i].getTileSize());
      log.info("Filtered split count = " + filteredSplits.size());
      nativeSplitsPerInput.add(filteredSplits);
      post.put(pyramid, pyramids[i].getBounds());
    }

    //TODO:  Combine smaller splits into a single, bigger split...
    List<InputSplit> splits = new LinkedList<InputSplit>();
    Map<String, Bounds> pre = new HashMap<String, Bounds>();
    for (int i=0; i < pyramids.length; i++)
    {
      MrsPyramid pyramid = pyramids[i];
      // remove the current bounds from the post bounds list.
      Bounds b = post.remove(pyramid.getName());
      // Loop the native splits from this input, and create a new MrsPyramidInputSplit
      // that wraps it and includes the pre/post bounds.
      List<TiledInputSplit> ns = nativeSplitsPerInput.get(i);
      if (ns != null)
      {
        Iterator<TiledInputSplit> iter = ns.iterator();
        while (iter.hasNext())
        {
          TiledInputSplit tiledSplit = iter.next();
          MrsPyramidInputSplit mpsplit =
              new MrsPyramidInputSplit(tiledSplit, pyramid.getName(), zooms[i],
                  pre.values().toArray(new Bounds[0]),
                  post.values().toArray(new Bounds[0]));
          splits.add(mpsplit);
        }
      }

      // add the current bounds to the pre bounds list
      pre.put(pyramid.getName(), b);
    }

    return splits;
  }

  /**
   * Add one new split to result for each row of tiles between fromTileId
   * and toTileId. The from and to tiles can be in the same row or different
   * rows.
   * 
   * @param result
   * @param fromTileId
   * @param toTileId
   * @param zoom
   * @param tileSize
   */
  private void fillHoles(List<TiledInputSplit> result, long fromTileId,
   long toTileId, int zoom, int tileSize, TMSUtils.TileBounds cropBounds)
  {
    TMSUtils.Tile fromTile = TMSUtils.tileid(fromTileId, zoom);
    TMSUtils.Tile toTile = TMSUtils.tileid(toTileId, zoom);
    long txStart = fromTile.tx;
    long tyStart = fromTile.ty;
    // If the end of the "from" tile is to the right of the crop bounds, then
    // move it up one row and start at the left side of the crop bounds.
    if (txStart > cropBounds.e)
    {
      txStart = cropBounds.w;
      tyStart = fromTile.ty + 1;
    }
    for (long ty = tyStart; ty <= toTile.ty; ty++)
    {
      long txEnd = -1;
      if (ty < toTile.ty)
      {
        txEnd = cropBounds.e;
      }
      else
      {
        // It's in the same row as the ending tile. If the ending tile is left of
        // the crop region, then we do not need to add a split for this row because
        // it does not overlap the crop region in this row.
        if (toTile.tx >= cropBounds.w)
        {
          txEnd = Math.min(toTile.tx, cropBounds.e);
        }
      }
      // Add the new split if one is needed (e.g. txEnd >= 0)
      if (txEnd >= 0 && txStart <= txEnd)
      {
        long startTileId = TMSUtils.tileid(txStart, ty, zoom);
        long endTileId = TMSUtils.tileid(txEnd, ty, zoom);
        result.add(new TiledInputSplit(null, startTileId, endTileId, zoom, tileSize));
      }
      txStart = cropBounds.w;
    }
  }

  /**
   * Performs cropping of input splits to the bounds specified in the ifContext. This
   * logic is common to all pyramid input formats, regardless of the data provider,
   * so there should be no need to override it in sub-classes.
   * 
   * @param ifContext
   * @param splits
   * @param zoomLevel
   * @param tileSize
   * @return
   */
  List<TiledInputSplit> filterInputSplits(final TiledInputFormatContext ifContext,
      final List<TiledInputSplit> splits, final int zoomLevel, final int tileSize)
  {
    // If there are no splits or no crop region, just return the splits
    if (splits.size() == 0 || ifContext.getBounds() == null)
    {
      return splits;
    }
    List<TiledInputSplit> result = new ArrayList<TiledInputSplit>();
    TMSUtils.TileBounds cropBounds = TMSUtils.boundsToTile(TMSUtils.Bounds.asTMSBounds(ifContext.getBounds()),
        ifContext.getZoomLevel(), tileSize);

    // sort the splits by tileid, so we can handle missing tiles in the case of a fill...S
    Collections.sort(splits, new Comparator<TiledInputSplit>()
    {
      @Override
      public int compare(TiledInputSplit t1, TiledInputSplit t2)
      {
        if (t1.getStartTileId() < t2.getStartTileId())
          return -1;
        return t1.getStartTileId() == t2.getStartTileId() ? 0 : 1;
      }
    });

    SplitIterator splitIter = new SplitIterator(splits, new RegionSplitVisitor(cropBounds));
    TiledInputSplit firstSplit = null;
    TiledInputSplit secondSplit = splitIter.next();
    long fromTileId = TMSUtils.tileid(cropBounds.w, cropBounds.s, ifContext.getZoomLevel());
    while (secondSplit != null)
    {
      long toTileId = secondSplit.getStartTileId() - 1;
      if (ifContext.getIncludeEmptyTiles()) {
        fillHoles(result, fromTileId, toTileId, ifContext.getZoomLevel(),
                ifContext.getTileSize(), cropBounds);
      }
      result.add(secondSplit);
      firstSplit = secondSplit;
      secondSplit = splitIter.next();
      fromTileId = firstSplit.getEndTileId() + 1;
    }

    // Post-processing to fill in holes beyond the last split but remaining
    // within the crop bounds
    if (ifContext.getIncludeEmptyTiles())
    {
      fillHoles(result, fromTileId,
          TMSUtils.tileid(cropBounds.e, cropBounds.n, zoomLevel),
          zoomLevel, tileSize, cropBounds);
    }
    return result;
  }
}
