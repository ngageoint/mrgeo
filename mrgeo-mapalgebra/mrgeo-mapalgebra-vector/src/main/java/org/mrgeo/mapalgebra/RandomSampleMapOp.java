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

import org.apache.hadoop.fs.Path;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.progress.Progress;
import org.mrgeo.progress.ProgressHierarchy;
import org.mrgeo.rasterops.RandomSamplesToTsv;
import org.mrgeo.utils.Bounds;

import java.io.IOException;
import java.util.Vector;

public class RandomSampleMapOp extends VectorMapOp implements BoundsCalculator
{
  private boolean usingRasterInput;
  private int sampleCount = 0;
  private String newColumnName;
  private String newColumnValue;
  private double minX;
  private double maxX;
  private double minY;
  private double maxY;
  private int zoomLevel;

  @Override
  public void addInput(MapOp n) throws IllegalArgumentException
  {
    if (_inputs.size() == 0)
    {
      if (!(n instanceof RasterMapOp))
      {
        throw new IllegalArgumentException("The first parameter must be a raster input.");
      }
      _inputs.add(n);
    }
    else
    {
      throw new IllegalArgumentException("Only one input is supported.");
    }
  }

  @Override
  public Bounds calculateBounds() throws IOException
  {
    if (usingRasterInput)
    {
      return MapAlgebraExecutioner.calculateBounds(_inputs.get(0));
    }
    return new Bounds(minX, minY, maxX, maxY);
  }


  @Override
  public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
  {
    int tileSize = Integer.parseInt(MrGeoProperties.getInstance()
        .getProperty(MrGeoConstants.MRGEO_MRS_TILESIZE, MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT));
    ProgressHierarchy ph = new ProgressHierarchy(p);
    ph.createChild(1.0f);
    ph.createChild(1.0f);

    Path outputColumns = new Path(getOutputName() + ".columns");
    Path op = new Path(_outputName);

    if (usingRasterInput)
    {
      if ((newColumnName != null) && (newColumnName.length() > 0) &&
          (newColumnValue != null)) {
        RandomSamplesToTsv.writeRandomPointsToTsv(ph.getChild(1), _inputs.get(0), sampleCount,
            newColumnName, newColumnValue, op, outputColumns);
      }
      else {
        RandomSamplesToTsv.writeRandomPointsToTsv(ph.getChild(1), _inputs.get(0), sampleCount,
            null, null, op, outputColumns);
      }
    }
    else
    {
      Bounds bounds = new Bounds(minX, minY, maxX, maxY);
      if ((newColumnName != null) && (newColumnName.length() > 0) &&
          (newColumnValue != null)) {
        RandomSamplesToTsv.writeRandomPointsToTsv(ph.getChild(1), bounds, zoomLevel, tileSize, sampleCount,
            newColumnName, newColumnValue, op, outputColumns);
      }
      else {
        RandomSamplesToTsv.writeRandomPointsToTsv(ph.getChild(1), bounds, zoomLevel, tileSize, sampleCount,
            null, null, op, outputColumns);
      }
    }
    _output = new BasicInputFormatDescriptor(_outputName);
  }

  @Override
  public void moveOutput(String toName) throws IOException
  {
    super.moveOutput(toName);
    _outputName = toName;
    _output = new BasicInputFormatDescriptor(_outputName);
  }

  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapter parser)
  {
    Vector<ParserNode> result = new Vector<ParserNode>();

    if (children.size() != 2 && children.size() != 4 && children.size() != 6 && children.size() != 8)
    {
      throw new IllegalArgumentException(
          "RandomSample usage: either (raster, sampleCount, [newColumnName, newColumnValue]) OR " +
          "(minX, minY, maxX, maxY, zoomLevel, sampleCount, [newColumnName, newColumnValue])");
    }

    if (children.size() <= 4)
    {
      // This is the usage with a raster input
      usingRasterInput = true;
      // First arg is the raster input
      result.add(children.get(0));

      // Second arg is the sampleCount
      sampleCount = parseChildInt(children.get(1), "sampleCount", parser);

      // Optional newColumn arguments
      if (children.size() > 2) {
        parseNewColumnParams(children, 2, parser);
      }
    }
    else
    {
      // This is the usage with bounds inputs
      usingRasterInput = false;
      minX = parseChildDouble(children.get(0), "minX", parser);
      minY = parseChildDouble(children.get(1), "minY", parser);
      maxX = parseChildDouble(children.get(2), "maxX", parser);
      maxY = parseChildDouble(children.get(3), "maxY", parser);
      zoomLevel = parseChildInt(children.get(4), "zoomLevel", parser);
      sampleCount = parseChildInt(children.get(5), "sampleCount", parser);
      // Optional newColumn arguments
      if (children.size() > 6) {
        parseNewColumnParams(children, 6, parser);
      }
    }
    return result;
  }


  private void parseNewColumnParams(Vector<ParserNode> children, int startParamIndex, ParserAdapter parser)
  {
    // Make sure the column value gets evaluated
    newColumnName = parseChildString(children.get(startParamIndex), "column " + startParamIndex, parser);
    if ((newColumnName == null) || (newColumnName.length() == 0)) {
      throw new IllegalArgumentException("The new column name cannot be blank. If you do " +
          "not wish to specify a new column, leave this argument out of the function call.");
    }
    newColumnValue = parseChildString(children.get(startParamIndex + 1), "column " + startParamIndex + 1, parser);
    if (newColumnValue == null) {
      throw new IllegalArgumentException("The new column value must be included when a column name is specified. If you do " +
          "not wish to specify a new column, leave this argument out of the function call.");
    }
  }

  @Override
  public String toString()
  {
    return String.format("RandomSampleMapOp %s",
        _outputName == null ? "null" : _outputName );
  }

}
