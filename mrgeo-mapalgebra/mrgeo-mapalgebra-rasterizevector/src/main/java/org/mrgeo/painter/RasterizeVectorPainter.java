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

package org.mrgeo.painter;

import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.image.ImageStats;
import org.mrgeo.paint.*;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.Tile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.Arrays;

public class RasterizeVectorPainter
{
  static final Logger log = LoggerFactory.getLogger(RasterizeVectorPainter.class);

  public enum AggregationType {
    SUM, MASK, LAST, MIN, MAX, AVERAGE
  }

  private AggregationType aggregationType;
  private String valueColumn;
  private int tileSize;
  private int zoom;
  private Bounds inputBounds = null;
private ImageStats[] stats = null;

  private GeometryPainter rasterPainter;
private Composite composite;
  private WritableRaster totalRaster;
  private WritableRaster raster;

  /**
   * Use this constructor if you need to use this class outside of the context of
   * a map/reduce 1 job.
   *
   */
  public RasterizeVectorPainter(int zoom, AggregationType aggregationType, String valueColumn,
                                int tileSize)
  {
    this.zoom = zoom;
    this.aggregationType = aggregationType;
    this.valueColumn = valueColumn;
    this.tileSize = tileSize;
  }

  public void beforePaintingTile(final long tileId)
  {
    if (aggregationType == AggregationType.MIN)
    {
      composite = new MinCompositeDouble();
    }
    else if (aggregationType == AggregationType.MAX)
    {
      composite = new MaxCompositeDouble();
    }
    else if (aggregationType == AggregationType.MASK)
    {
      // When a feature is painted in MASK mode, the src raster will contain
      // 1.0 in each pixel that overlaps the feature. The MaskComposite will
      // write a 0.0 to each pixel that was originally painted, and if any
      // polygon holes are painted after that, those are written as NaN.
      composite = new MaskComposite(1.0, 0.0, Double.NaN);
    }
    else
    {
      composite = new AdditiveCompositeDouble();
    }

    // Because of how MASK works, and having to properly handle inner rings of
    // polygons, the raster is initialized to all 0's for the MASK aggregation.
    // There is no way to paint NaN values onto the raster using the GeometryPainter
    // so the non-masked pixels will be set to a value of 0 (which can be painted).
    raster = RasterUtils.createEmptyRaster(tileSize, tileSize, 1,
        DataBuffer.TYPE_DOUBLE, Double.NaN);

    totalRaster = null;
    GeometryPainter totalPainter = null;

    if (aggregationType == AggregationType.AVERAGE)
    {
      totalRaster = raster.createCompatibleWritableRaster();

      final BufferedImage bi = RasterUtils.makeBufferedImage(totalRaster);
      final Graphics2D gr = bi.createGraphics();

      gr.setComposite(composite);
      gr.setStroke(new BasicStroke(0));

      totalPainter = new GeometryPainter(gr, totalRaster, new Color(1, 1, 1), new Color(0, 0, 0));
    }

    final BufferedImage bi = RasterUtils.makeBufferedImage(raster);
    final Graphics2D gr = bi.createGraphics();

    gr.setComposite(composite);
    gr.setStroke(new BasicStroke(0));

    rasterPainter = new GeometryPainter(gr, raster, new Color(1, 1, 1),
        new Color(0, 0, 0));

    final Tile tile = TMSUtils.tileid(tileId, zoom);
    final TMSUtils.Bounds tb = TMSUtils.tileBounds(tile.tx, tile.ty, zoom, tileSize);
    Bounds b = new Bounds(tb.w, tb.s, tb.e, tb.n);
    rasterPainter.setBounds(b);
  }

  public void paintGeometry(Geometry g)
  {
    final Bounds featureBounds = g.getBounds();
    if (inputBounds == null)
    {
      inputBounds = featureBounds;
    }
    else
    {
      inputBounds.expand(featureBounds);
    }

    if (valueColumn == null || aggregationType == AggregationType.MASK)
    {
      rasterPainter.paint(g);
    }
    else
    {
      final String sv = g.getAttribute(valueColumn);
      if (sv != null)
      {
        final double v = Double.parseDouble(sv);
        ((WeightedComposite)composite).setWeight(v);

        rasterPainter.paint(g);

      }
      else
      {
        log.info("Ignoring feature because there is no column: " + valueColumn);
      }
    }
  }

//  public boolean afterPaintingGeometry(Geometry g)
//  {
//
//    // nothing else to do...
//    if (aggregationType == AggregationType.LAST)
//    {
//      return false;
//    }
//
//    if (aggregationType == AggregationType.AVERAGE && totalPainter != null)
//    {
//      ((WeightedComposite)composite).setWeight(1.0);
//      totalPainter.setBounds(b);
//      totalPainter.paint(g);
//    }
//    return true;
//  }

  public RasterWritable afterPaintingTile() throws IOException
  {
    if (aggregationType == AggregationType.AVERAGE)
    {
      averageRaster(raster, totalRaster);
    }

    if (stats != null)
    {
      // compute stats on the tile and update aggregate stats
      final ImageStats[] tileStats = ImageStats.computeStats(raster, new double[] { Double.NaN });
      stats = ImageStats.aggregateStats(Arrays.asList(stats, tileStats));
    }

    return RasterWritable.toWritable(raster);
  }

  public ImageStats[] getStats()
  {
    return stats;
  }

  private static void averageRaster(final WritableRaster raster, final Raster count)
  {
    for (int y = 0; y < raster.getHeight(); y++)
    {
      for (int x = 0; x < raster.getWidth(); x++)
      {
        for (int b = 0; b < raster.getNumBands(); b++)
        {
          double v = raster.getSampleDouble(x, y, b);
          final double c = count.getSampleDouble(x, y, b);

          if (!Double.isNaN(v))
          {
            if (c == 0.0)
            {
              v = Double.NaN;
            }
            else
            {
              v /= c;
            }

            raster.setSample(x, y, b, v);
          }
        }
      }
    }
  }
}
