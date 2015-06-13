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

package org.mrgeo.mapalgebra;

import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.opimage.CropRasterDescriptor;
import org.mrgeo.opimage.CropRasterOpImage;
import org.mrgeo.progress.Progress;
import org.mrgeo.utils.Bounds;

import java.io.IOException;
import java.util.Vector;

/**
 * Crop the input image down to a smaller region. This is done in an intelligent
 * fashion to try and avoid excess processing
 */
public class CropRasterMapOp extends RenderedImageMapOp implements BoundsCalculator
{
  private String _cropType = CropRasterOpImage.FAST;
  private Bounds actualBounds = new Bounds();

  public CropRasterMapOp()
  {
    _factory = new CropRasterDescriptor();
  }

  public CropRasterMapOp(Bounds bounds, int zoomLevel, int tileSize)
  {
    actualBounds = bounds;
    _factory = new CropRasterDescriptor();
    getParameters().add(actualBounds.getMinX());
    getParameters().add(actualBounds.getMinY());
    getParameters().add(actualBounds.getWidth());
    getParameters().add(actualBounds.getHeight());
    getParameters().add(zoomLevel);
    getParameters().add(tileSize);
    getParameters().add(CropRasterOpImage.FAST);
  }


  public static String[] register()
  {
    return new String[] { "crop" };
  }

  @Override
  public void addInput(MapOp n) throws IllegalArgumentException
  {
    if (!(n instanceof RasterMapOp))
    {
      throw new IllegalArgumentException("Only raster inputs are supported.");
    }
    if (_inputs.size() != 0)
    {
      throw new IllegalArgumentException("Only one input is supported.");
    }
    _inputs.add(n);
  }

  @Override
  public Bounds calculateBounds() throws IOException
  {
    return actualBounds;
  }

  @Override
  public void build(Progress p) throws IOException,
  JobFailedException, JobCancelledException
  {
    // Replace the placeholder parameters with the actual values now that the
    // _inputs are resolved.

    int zoomlevel = MapAlgebraExecutioner.calculateMaximumZoomlevel(this);
    int tilesize = MapAlgebraExecutioner.calculateTileSize(this);

    // if we don't have a zoomlevel, see if we can walk
    MapOp mop = getParent();
    while (mop != null && zoomlevel <= 0)
    {
      zoomlevel =  MapAlgebraExecutioner.calculateMaximumZoomlevel(mop);
      if (tilesize <= 0)
      {
        tilesize = MapAlgebraExecutioner.calculateTileSize(mop);
      }
      mop = mop.getParent();
    }
    if (zoomlevel <= 0)
    {
      zoomlevel = 1;
    }

    while (mop != null && tilesize <= 0)
    {
      tilesize = MapAlgebraExecutioner.calculateTileSize(mop);
      mop = mop.getParent();
    }

    if (tilesize <= 0)
    {
      tilesize = Integer.parseInt(MrGeoProperties.getInstance().getProperty("mrsimage.tilesize", "512"));
    }

    getParameters().set(zoomlevel, 4);
    getParameters().set(tilesize, 5);

    super.build(p);
  }


  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapter parser)
  {
    Vector<ParserNode> result = new Vector<ParserNode>();

    if (children.size() > 6)
    {
      throw new IllegalArgumentException(
          "crop takes five or six arguments. (source, minx, miny, maxx, and maxy, or crop type)");
    }

    result.add(children.get(0));

    double[] b = new double[4];

    int slot = 0;
    for (int i = 1; i < 5; i++)
    {
      b[slot++] = parseChildDouble(children.get(i), "bounds", parser);
    }
    actualBounds = new Bounds(b[0], b[1], b[2], b[3]);

    if (children.size() == 6)
    {
      _cropType = parseChildString(children.get(5), "crop type", parser).toUpperCase();
      if (!_cropType.equals(CropRasterOpImage.FAST) && !_cropType.equals(CropRasterOpImage.EXACT))
      {
        throw new IllegalArgumentException(String.format(
            "Invalid value for crop type (%s). It must be '%s' or '%s'.",
            _cropType, CropRasterOpImage.FAST, CropRasterOpImage.EXACT));
      }
      double x = actualBounds.getMinX();
      double y = actualBounds.getMinY();
      double width = actualBounds.getWidth();
      double height = actualBounds.getHeight();
      getParameters().add(x);
      getParameters().add(y);
      getParameters().add(width);
      getParameters().add(height);
      getParameters().add(0); // placeholder for zoom
      getParameters().add(0); // placeholder for tileSize
      getParameters().add(_cropType);
    }
    else if (children.size() == 5)
    {
      double x = actualBounds.getMinX();
      double y = actualBounds.getMinY();
      double width = actualBounds.getWidth();
      double height = actualBounds.getHeight();
      getParameters().add(x);
      getParameters().add(y);
      getParameters().add(width);
      getParameters().add(height);
      getParameters().add(0); // placeholder for zoom
      getParameters().add(0); // placeholder for tileSize
      getParameters().add(CropRasterOpImage.FAST);
    }

    return result;
  }

  @Override
  public String toString()
  {
    return "crop(" + actualBounds.toString() + ")";
  }
}
