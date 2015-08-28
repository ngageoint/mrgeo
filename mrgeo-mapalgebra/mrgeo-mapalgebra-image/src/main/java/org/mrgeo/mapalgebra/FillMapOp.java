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

package org.mrgeo.mapalgebra;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.FillRasterDriver;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.opimage.CropRasterOpImage;
import org.mrgeo.opimage.MrsPyramidDescriptor;
import org.mrgeo.progress.Progress;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FillMapOp extends RasterMapOp implements InputsCalculator, BoundsCalculator
{
  private static final Logger log = LoggerFactory.getLogger(FillMapOp.class);

  private Bounds bounds = null;
  private String filltype = CropRasterOpImage.FAST;

  private RasterMapOp source = null;
  private double fill = Double.NaN;

  public FillMapOp()
  {
  }

  public static String[] register()
  {
    return new String[] { "fill" };
  }

  @Override
  public void addInput(final MapOpHadoop n) throws IllegalArgumentException
  {
    if (source == null)
    {
      if (!(n instanceof RasterMapOp))
      {
        throw new IllegalArgumentException("Only raster inputs are supported for the source input.");
      }
      source = (RasterMapOp)n;
      _inputs.add(source);
    }
  }

  @Override
  public Bounds calculateBounds() throws IOException
  {
    if (bounds == null)
    {
      bounds = Bounds.world;
      if (source != null)
      {
        bounds = MapAlgebraExecutioner.calculateBounds(source);
        if (filltype == CropRasterOpImage.FAST)
        {
          // need to expand the bounds to the tile bounds, because we're filling in the nodata
          // around the edges
          final int zoom = MapAlgebraExecutioner.calculateMaximumZoomlevel(source);
          final int tilesize = MapAlgebraExecutioner.calculateTileSize(source);

          bounds = TMSUtils.tileBounds(bounds.getTMSBounds(), zoom, tilesize)
            .convertNewToOldBounds();
        }
      }
    }

    return bounds;
  }

  @Override
  public Set<String> calculateInputs()
  {
    Set<String> inputPyramids = new HashSet<String>();
    if (_outputName != null)
    {
      inputPyramids.add(_outputName.toString());
    }
    return inputPyramids;
  }

  @Override
  public void moveOutput(final String toPath) throws IOException
  {
    super.moveOutput(toPath);
    _outputName = toPath;
    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(_outputName,
        AccessMode.READ, getProviderProperties());
    _output = MrsPyramidDescriptor.create(dp);
  }

  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapter parser)
  {
    final Vector<ParserNode> result = new Vector<ParserNode>();

    if (children.size() < 2)
    {
      throw new IllegalArgumentException(
        "FillMapOp usage: fill(source raster, fill value, [minX, minY, maxX, maxY], [fill type])");
    }

    result.add(children.get(0));

    fill = parseChildDouble(children.get(1), "fill value", parser);
    if (children.size() > 2)
    {
      if (children.size() == 3)
      {
        getFillType(children, 2, parser);
      }
      else if (children.size() >= 6)
      {
        final double[] b = new double[4];

        int slot = 0;
        for (int i = 2; i < 6; i++)
        {
          b[slot++] = parseChildDouble(children.get(i), "bounds", parser);
        }
        bounds = new Bounds(b[0], b[1], b[2], b[3]);

        if (children.size() == 7)
        {
          getFillType(children, 6, parser);

        }
      }
      else
      {
        throw new IllegalArgumentException(
          "FillMapOp usage: full(source raster, fill value, [minX, minY, maxX, maxY], [fill type])");
      }
    }

    return result;
  }

  @Override
  public String toString()
  {
    return String.format("fill bounds: %s %s", bounds == null ? "unk" : bounds.toString(),
      source == null ? "null" : source.toString());

    // return "fill(" + (bounds == null ? "null" : bounds.toString()) + ")";
  }

  @Override
  public void build(final Progress p) throws IOException, JobFailedException,
    JobCancelledException
  {
    if (p != null)
    {
      p.starting();
    }

    final MrsImagePyramid sourcepyramid = RasterMapOp.flushRasterMapOpOutput(source, 0);

    log.info("FillMapOp output path: " + _outputName.toString());

    final Job job = new Job(createConfiguration());

    if (bounds == null)
    {
      bounds = calculateBounds();
    }

    FillRasterDriver.run(job, sourcepyramid, _outputName, fill, filltype, bounds, p,
      jobListener, getProtectionLevel(), getProviderProperties());
    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(_outputName,
        AccessMode.READ, getProviderProperties());
    _output = MrsPyramidDescriptor.create(dp);

    if (p != null)
    {
      p.complete();
    }

  }

  private void getFillType(final Vector<ParserNode> children, final int offset, final ParserAdapter parser)
  {
    filltype = parseChildString(children.get(offset), "fill type", parser);

    // we're just borrowing the types from the CropRasterOpImage, we don't actually use the OpImage
    if (!filltype.equals(CropRasterOpImage.FAST) && !filltype.equals(CropRasterOpImage.EXACT))
    {
      throw new IllegalArgumentException(String.format(
        "Invalid value for fill type (%s). It must be '%s' or '%s'.", filltype,
        CropRasterOpImage.FAST, CropRasterOpImage.EXACT));
    }
  }
}
