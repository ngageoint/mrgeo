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

import org.mrgeo.mapreduce.formats.TileClusterInfo;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.opimage.MrsPyramidDescriptor;
import org.mrgeo.progress.Progress;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.utils.Bounds;

import java.awt.image.RenderedImage;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class MrsPyramidMapOp extends RasterMapOp
  implements InputsCalculator, BoundsCalculator, TileSizeCalculator,
  MaximumZoomLevelCalculator, TileClusterInfoConsumer
{
  MrsImageDataProvider dp;
  private TileClusterInfo overallTileClusterInfo;

  @Override
  public void addInput(MapOp n) throws IllegalArgumentException
  {
    throw new IllegalArgumentException("MrsPyramidMapOp doesn't take any inputs.");
  }
  
  public static String[] register()
  {
    // returning a "" means don't register this MapOp
    return new String[] { "" };
  }


  @Override
  public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
  {
    if (p != null)
    {
      p.starting();
    }
    //    RenderedImage result = ReplaceableRasterDescriptor.create(pyramidName, pyramid.getBounds());

    //  RenderedImage result = GeographicWritableDescriptor.create(MrsPyramidv1.loadPyramid(new Path(pyramidName))
    //  .getImage(0).getPath().toString());
    //    
    //    if (result.getSampleModel().getDataType() != DataBuffer.TYPE_FLOAT)
    //    {
    //      result = ConvertToFloatDescriptor.create(result);
    //    }
    //    result = TileCacheDescriptor.create(result);
    //  
    //  
    _output = getOutputForHighestLevel();

    if (p != null)
    {
      p.complete();
    }
  }

  @Override
  public void moveOutput(String toName) throws IOException
  {
//    DataProviderFactory.move(getOutputName(), toName, true);
    dp = DataProviderFactory.getMrsImageDataProvider(getOutputName(), AccessMode.READ,
        getProviderProperties());
    if (dp != null)
    {
      dp.move(toName);
    }
//    String sourceName = getOutputName();
//    Configuration conf = HadoopUtils.createConfiguration();
//    Path toPath = new Path(toName);
//    Path sourcePath = new Path(sourceName);
//    FileSystem sourceFs = HadoopFileUtils.getFileSystem(conf, sourcePath);
//    FileSystem destFs = HadoopFileUtils.getFileSystem(conf, toPath);
//    if (!FileUtil.copy(sourceFs, sourcePath, destFs, toPath, false, false, conf))
//    {
//      throw new IOException("Error copying '" + sourceName.toString() +
//          "' to '" + toName.toString() + "'");
//    }
    _outputName = toName;
    _output = getOutputForHighestLevel();
  }

  @Override
  public RenderedImage getRasterOutput() throws IOException
  {
    return getOutputForHighestLevel();
  }

  public RenderedImage getOutputForLevel(int level)
  {
    return MrsPyramidDescriptor.create(dp, level, overallTileClusterInfo,
        getProviderProperties());
  }

  public RenderedImage getOutputForHighestLevel() throws IOException
  {
    return getOutputForLevel(dp.getMetadataReader().read().getMaxZoomLevel());
  }

  @Override
  public String getOutputName()
  {
    return this.dp.getResourceName();
//    return getOutputPathForHighestLevel();
  }

  public void setDataProvider(final MrsImageDataProvider dp)
  {
    this.dp = dp;
  }

  public MrsImageDataProvider getDataProvider()
  {
    return dp;
  }

  @Override
  public Bounds calculateBounds() throws IOException
  {
    if (dp != null)
    {
      return dp.getMetadataReader().read().getBounds();
    }
    return Bounds.world;
  }

  @Override
  public int calculateMaximumZoomlevel() throws IOException
  {
    if (dp != null)
    {
      return dp.getMetadataReader().read().getMaxZoomLevel();
    }
    return 0;
  }
  
  @Override
  public int calculateTileSize() throws IOException
  {
    return dp.getMetadataReader().read().getTilesize();
  }

  @Override
  public Set<String> calculateInputs()
  {
    Set<String> inputPyramids = new HashSet<String>();
    inputPyramids.add(dp.getResourceName());

    return inputPyramids;
  }

  @Override
  public void setOverallTileClusterInfo(TileClusterInfo tileClusterInfo)
  {
    this.overallTileClusterInfo = tileClusterInfo;
  }

  @Override
  public MrsPyramidMapOp clone()
  {
    MrsPyramidMapOp result = (MrsPyramidMapOp) super.clone();
    result.setDataProvider(dp);
    return result;
  }

  @Override
  public String toString()
  {
    if (dp != null)
    {
      return "[" + dp.getResourceName() + "]";
    }
    return "[unknown]";
  }

}
