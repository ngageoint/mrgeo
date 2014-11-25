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

package org.mrgeo.data;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.mapreduce.formats.TileCollection;
import org.mrgeo.mapreduce.splitters.MrsPyramidInputSplit;
import org.mrgeo.mapreduce.splitters.TiledInputSplit;
import org.mrgeo.pyramid.MrsPyramid;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.data.tile.TiledInputFormatContext;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.TMSUtils;

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
      nativeSplitsPerInput.add(filterInputSplits(ifContext,
          getNativeSplits(context, ifContext, pyramid), zooms[i],
          pyramids[i].getTileSize()));
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
    List<TiledInputSplit> result = new ArrayList<TiledInputSplit>();
    TMSUtils.TileBounds cropBounds = null;

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


    TMSUtils.Tile lastTile = null;

    if (ifContext.getBounds() != null)
    {
      cropBounds = TMSUtils.boundsToTile(TMSUtils.Bounds.asTMSBounds(ifContext.getBounds()), ifContext.getZoomLevel(), tileSize);;
      lastTile = new TMSUtils.Tile(cropBounds.w, cropBounds.s);
    }

    for (TiledInputSplit tiledSplit : splits)
    {
      // If the caller requested bounds cropping, then check the tile bounds for intersection
      if (cropBounds != null)
      {
        // See if the split bounds intersects the crop
        TMSUtils.Tile splitStartTile = TMSUtils.tileid(tiledSplit.getStartTileId(), ifContext.getZoomLevel());
        TMSUtils.Tile splitEndTile = TMSUtils.tileid(tiledSplit.getEndTileId(), ifContext.getZoomLevel());
        TMSUtils.TileBounds splitTileBounds = new TMSUtils.TileBounds(
            splitStartTile.tx, splitStartTile.ty, splitEndTile.tx, splitEndTile.ty);

//        TMSUtils.Bounds splitBounds = TMSUtils.tileToBounds(splitTileBounds,
//            zoomLevel, tileSize);

        TMSUtils.TileBounds intersection = cropBounds.intersection(splitTileBounds);
        if (intersection != null)
        {
          long startId = TMSUtils.tileid(intersection.w, intersection.s, ifContext.getZoomLevel());
          long endId = TMSUtils.tileid(intersection.e, intersection.n, ifContext.getZoomLevel());

          long lastTileId = TMSUtils.tileid(lastTile.tx, lastTile.ty, ifContext.getZoomLevel());

          if (ifContext.getIncludeEmptyTiles() && lastTileId < startId)
          {
            startId = lastTileId;
          }

          // if we intersect at the right bounds, we need to move the last tile to the next row
          // in the cropped bounds, otherwise, it's just the next tile
          if (intersection.e >= cropBounds.e)
          {
            lastTile = new TMSUtils.Tile(cropBounds.w, intersection.n + 1);
          }
          else
          {
            lastTile = new TMSUtils.Tile(intersection.e, intersection.n);
          }

          result.add(new  TiledInputSplit(tiledSplit.getWrappedSplit(), startId,
              endId, tiledSplit.getZoomLevel(), tiledSplit.getTileSize()));
        }
      }
      else
      {
        result.add(tiledSplit);
      }
    }

    // check if we need to add additional tiles to the last split...
    if (ifContext.getIncludeEmptyTiles() && cropBounds != null)
    {
      long finalId = TMSUtils.tileid(cropBounds.e, cropBounds.n, ifContext.getZoomLevel());

      long lastTileId = TMSUtils.tileid(lastTile.tx, lastTile.ty, ifContext.getZoomLevel());
      if (finalId > lastTileId)
      {
        if (result.size() > 0)
        {
          TiledInputSplit last = result.get(result.size() - 1);

          result.set(result.size() - 1, new TiledInputSplit(last.getWrappedSplit(), last.getStartTileId(),
              finalId, last.getZoomLevel(), last.getTileSize()));
        }
        else
        {
          // nothing put in the result, this means we'll have all blank tiles returned
          result.add(new TiledInputSplit(null, lastTileId,
              finalId, ifContext.getZoomLevel(), ifContext.getTileSize()));
        }
      }
    }
    return result;
  }
}
