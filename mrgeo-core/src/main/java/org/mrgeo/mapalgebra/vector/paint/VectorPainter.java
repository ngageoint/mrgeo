/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.mapalgebra.vector.paint;

import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.data.raster.RasterUtils;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.Point;
import org.mrgeo.utils.FloatUtils;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.TMSUtils;
import org.mrgeo.utils.tms.Tile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;

public class VectorPainter
{
private static final Logger log = LoggerFactory.getLogger(VectorPainter.class);

public enum AggregationType {
  SUM, MASK, MIN, MAX, AVERAGE, GAUSSIAN, MASK2
}

private AggregationType aggregationType;
private String valueColumn;
private int tileSize;
private int zoom;

private GeometryPainter rasterPainter;
private Composite composite;
private WritableRaster raster;
private GeometryPainter totalPainter = null;
private WritableRaster totalRaster;

/**
 * Use this constructor if you need to use this class outside of the context of
 * a map/reduce 1 job.
 *
 */
public VectorPainter(int zoom, AggregationType aggregationType, String valueColumn,
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
    composite = new MaskComposite(1.0, 0.0, Float.NaN);
  }
  else if (aggregationType == AggregationType.MASK2)
  {
    // A secondary mask type, 1.0 for overlap, 0.0 for none
    // When a feature is painted in MASK mode, the src raster will contain
    // 1.0 in each pixel that overlaps the feature. The MaskComposite will
    // write a 1.0 to each pixel that was originally painted, and if any
    // polygon holes are painted after that, those are written as 0.0.
    composite = new MaskComposite(1.0, 1.0, 0.0);
  }
  else if (aggregationType == AggregationType.GAUSSIAN)
  {
    composite = new GaussianComposite();
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
      DataBuffer.TYPE_FLOAT, Float.NaN);

  totalRaster = null;

  final Tile tile = TMSUtils.tileid(tileId, zoom);
  final Bounds tb = TMSUtils.tileBounds(tile.tx, tile.ty, zoom, tileSize);
  Bounds b = new Bounds(tb.w, tb.s, tb.e, tb.n);
  if (aggregationType == AggregationType.AVERAGE)
  {
    totalRaster = raster.createCompatibleWritableRaster();

    final BufferedImage bi = RasterUtils.makeBufferedImage(totalRaster);
    final Graphics2D gr = bi.createGraphics();

    gr.setComposite(new AdditiveComposite());
    gr.setStroke(new BasicStroke(0));

    totalPainter = new GeometryPainter(gr, totalRaster, new Color(1, 1, 1), new Color(0, 0, 0));
    totalPainter.setBounds(b);
  }

  final BufferedImage bi = RasterUtils.makeBufferedImage(raster);
  final Graphics2D gr = bi.createGraphics();

  gr.setComposite(composite);
  gr.setStroke(new BasicStroke(0));

  rasterPainter = new GeometryPainter(gr, raster, new Color(1, 1, 1),
      new Color(0, 0, 0));
  rasterPainter.setBounds(b);
}

public void paintGeometry(Geometry g)
{
  if (valueColumn == null || aggregationType == AggregationType.MASK || aggregationType == AggregationType.MASK2)
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

      if (aggregationType == AggregationType.AVERAGE && totalPainter != null)
      {
        totalPainter.paint(g);
      }
    }
    else
    {
      log.info("Ignoring feature because there is no column: " + valueColumn);
    }
  }
}


public void paintEllipse(Point center, double majorWidth, double minorWidth, double orientation, double weight) {

  if (composite instanceof WeightedComposite)
  {
    ((WeightedComposite) composite).setWeight(weight);
  }
  if (composite instanceof GaussianComposite)
  {
    ((GaussianComposite) composite).setEllipse(center, majorWidth, minorWidth, orientation, rasterPainter.getTransform());
  }


  rasterPainter.paintEllipse(center, majorWidth, minorWidth, orientation);
}

public RasterWritable afterPaintingTile() throws IOException
{

  if (aggregationType == AggregationType.AVERAGE)
  {
    averageRaster(raster, totalRaster);
  }

  int type = raster.getTransferType();
  double nodata = Float.NaN;
  if (aggregationType == AggregationType.MASK || aggregationType == AggregationType.MASK2)
  {
    type = DataBuffer.TYPE_BYTE;

    nodata = RasterUtils.getDefaultNoDataForType(DataBuffer.TYPE_BYTE);

  }

  MrGeoRaster mrgeo = MrGeoRaster.createEmptyRaster(raster.getWidth(), raster.getHeight(),
      raster.getNumBands(), type);

  for (int y = 0; y < raster.getHeight(); y++)
  {
    for (int x = 0; x < raster.getWidth(); x++)
    {
      if (aggregationType == AggregationType.MASK)
      {
        float v = raster.getSampleFloat(x, y, 0);
        if (Float.isNaN(v))
        {
          mrgeo.setPixel(x, y, 0, nodata);
        }
        else
        {
          mrgeo.setPixel(x, y, 0, v);
        }
      }
      else {
        mrgeo.setPixel(x, y, 0, raster.getSampleFloat(x, y, 0));
      }
    }
  }

  return RasterWritable.toWritable(mrgeo);
}


private static void averageRaster(final WritableRaster raster, final Raster count)
{
  for (int y = 0; y < raster.getHeight(); y++)
  {
    for (int x = 0; x < raster.getWidth(); x++)
    {
      for (int b = 0; b < raster.getNumBands(); b++)
      {
        float v = raster.getSampleFloat(x, y, b);
        final float c = count.getSampleFloat(x, y, b);

        if (!Float.isNaN(v))
        {
          if (FloatUtils.isEqual(c, 0.0))
          {
            v = Float.NaN;
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
