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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.DataProviderNotFound;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.mapreduce.formats.TileClusterInfo;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.opchain.OpChainDriver;
import org.mrgeo.opimage.ConstantDescriptor;
import org.mrgeo.opimage.TileCacheDescriptor;
import org.mrgeo.progress.Progress;
import org.mrgeo.utils.DependencyLoader;

import javax.media.jai.JAI;
import javax.media.jai.RenderedOp;
import java.awt.*;
import java.awt.image.RenderedImage;
import java.awt.image.renderable.ParameterBlock;
import java.awt.image.renderable.RenderedImageFactory;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class RenderedImageMapOp extends RasterMapOp implements DeferredExecutor, TileClusterInfoConsumer
{
  private RenderingHints _hints = JAI.getDefaultInstance().getRenderingHints();
  private ParameterBlock _param = new ParameterBlock();
  RenderedImageFactory _factory = null;
  String _opName = null;
  private boolean _useCache = false;
  private TileClusterInfo tileClusterInfo;
  private Configuration conf;

  @Override
  public void addInput(MapOp n) throws IllegalArgumentException
  {
    if (!(n instanceof RasterMapOp))
    {
      throw new IllegalArgumentException("Only raster inputs are supported, got a " + n.getClass().getName());
    }
    _inputs.add(n);
  }

  public ParameterBlock getParameters()
  {
    return _param;
  }

  public RenderedImageFactory getRenderedImageFactory()
  {
    return _factory;
  }

  @Override
  public void build(final Progress p) throws IOException, JobFailedException, JobCancelledException
  {
    try
    {
      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(getOutputName(),
          AccessMode.READ, getProviderProperties());
      if (dp != null)
      {
        dp.delete();
      }
    }
    catch(DataProviderNotFound e)
    {
      // ignore - it means the image didn't exist
    }

    // setup the list of inputs for the map/reduce 
    Set<String> inputs = new HashSet<String>();
    MapOp rootMapOp = findRoot();
    MapAlgebraExecutioner.calculateInputs(rootMapOp, inputs); 

    org.mrgeo.utils.HadoopUtils.setTileClusterInfo(getConf(), tileClusterInfo);

    OpChainDriver.opchain(getRasterOutput(), inputs, getOutputName(),
        MapAlgebraExecutioner.calculateMaximumZoomlevel(rootMapOp),
        MapAlgebraExecutioner.calculateBounds(rootMapOp),
        getConf(), p, getProtectionLevel(), getProviderProperties());

  }

  @Override
  public void prepare(final Progress p) throws IOException
  {
    if (p != null)
    {
      p.starting();
    }
    
    _output = createOutput();
    
    if (p != null)
    {
      p.complete();
    }
  }

  private RenderedImage createOutput() throws IOException
  {
    RenderedImage result = _createRenderedOp();

    if (_useCache)
    {
      RenderingHints hints = (RenderingHints) _hints.clone();
      hints.put(JAI.KEY_TILE_CACHE, JAI.getDefaultInstance().getTileCache());
      result = TileCacheDescriptor.create(result, -1, hints);
    }
    return result;
  }

  @Override
  public void moveOutput(String toName) throws IOException
  {
    super.moveOutput(toName);
    _outputName = toName;
    _output = createOutput();
  }

private Set<String> getDependencies(RenderedImageMapOp rop)
{
  Set<String> deps = new HashSet<>();
  try
  {
    deps.addAll(DependencyLoader.getDependencies(rop._factory.getClass()));

    for (MapOp op : rop.getInputs())
    {
      if (op instanceof RenderedImageMapOp)
      {
        deps.addAll(getDependencies((RenderedImageMapOp) op));
      }
    }
  }
  catch (IOException e)
  {
    e.printStackTrace();
  }

  return deps;
}

  private RenderedOp _createRenderedOp() throws IOException
  {
    if (_factory == null)
    {
      throw new IllegalArgumentException("The RenderedImageFactory must be specified.");
    }

    // Need to add dependencies for OpImage descriptors so that required JARs
    // are pushed to the data node side during a map/reduce. Otherwise,
    // a NullPointerException will be thrown when the OpChainDriver attempts
    // to instantiate the operation chain on the mapper side.
    //DependencyLoader.addDependencies(getConf(), _factory.getClass());
    if (!(getParent() instanceof RenderedImageMapOp))
    {
      DependencyLoader.copyDependencies(getDependencies(this));
    }

    // Reuse the parameters that the caller has already set up. But at
    // the end of the parameter list, we need to include the NoData value
    // for each source followed by the NoData value for this map op's
    // output (which for now will be the same as the NoData value for
    // the first input).
    // TODO: We assume the use of band 0 at this point, but that will change...
    ParameterBlock jaiParams = (ParameterBlock)_param.clone();
    if (jaiParams.getSources().size() == 0)
    {
      for (MapOp op : _inputs)
      {
        jaiParams.addSource(((RasterMapOp)op).getRasterOutput());
      }
    }
    if (includeFunctionNameInParameters())
    {
      jaiParams.add(getFunctionName());
    }
    // The ConstantOpImage requires a tilesize in its constructor, but
    // the MapAlgebraParser does not have enough information at the time
    // this map was constructed (to set the tilesize), so we pass the tilesize
    // on the fly here when creating the ConstantOpImage.
    if (_factory instanceof ConstantDescriptor)
    {
      int tilesize = MapAlgebraExecutioner.calculateTileSize(findRoot());
      if (tilesize <= 0)
      {
        tilesize = Integer.parseInt(MrGeoProperties.getInstance().getProperty("tilesize", MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT));
      }
      jaiParams.add(tilesize);
    }

    return JAI.create(_factory.getClass().getName(), jaiParams, _hints);
  }

  public void setRenderedImageFactory(RenderedImageFactory f)
  {
    _factory = f;
  }

  public void setUseCache(boolean b)
  {
    _useCache = b;
  }

  @Override
  public String toString()
  {
    String result = null;
    // make some special cases a little easier to read.
    if (_factory instanceof ConstantDescriptor && _param.getParameters().size() == 1)
    {
      result = _param.getObjectParameter(0).toString();
    }
    else
    {
      String paramStr = StringUtils.join(_param.getParameters(), ", ");
      result = _factory.toString() + (paramStr.isEmpty() ? "" : ("(" + paramStr + ")"));
    }
    return result;
  }

  @Override
  public RenderedImageMapOp clone()
  {
    RenderedImageMapOp result = (RenderedImageMapOp) super.clone();

    result._hints = (RenderingHints) _hints.clone();
    result._param = (ParameterBlock) _param.clone();
    // these shouldn't be changed so we can reference the original rather than a copy
    result._factory = _factory;
    result._opName = _opName;
    result._useCache = _useCache;

    return result;
  }

  /**
   * Sub-classes should override this method and return true if the actual
   * MapOp class is used for more than one map algebra function. When this
   * function returns true, the map algebra parser adds the function name
   * to the list of parameters passed to the associated OpImage.
   * 
   * @return
   */
  public boolean includeFunctionNameInParameters()
  {
    return false;
  }

  @Override
  public void setOverallTileClusterInfo(TileClusterInfo tileClusterInfo)
  {
    this.tileClusterInfo = tileClusterInfo;
  }

  @Override
  public String getOperationId()
  {
    return RenderedImageMapOp.class.getCanonicalName();
  }
  
  /**
   * Return an instance of a Hadoop Configuration to use for executing the
   * OpChainDriver. RenderedImageMapOp is a special type of MapOp in that it
   * wraps a JAI OpImage. JAI is capable of executing a "chain" of OpImages,
   * and so a sub-tree of the overall MapOp tree containing only
   * instances of RenderedImageMapOps can be executed in JAI via a single
   * Hadoop job using the OpChainDriver.
   * 
   * In order for that to work properly, the job setup performed for each
   * of the MapOps in the sub-tree must use the same Hadoop Configuration
   * instance. This is especially required for configuring the JAR dependencies
   * for the Hadoop job to ensure that all the code needed to execute the
   * sub-tree is included when the job is submitted.
   * 
   * In order to achieve a single Configuration instance for each sub-tree
   * containing only RenderedImageMapOp instances, this method is called to
   * get the Configuration instance. It runs through each of this MapOp's
   * parents until it gets to the last one that is also a RenderedImageMapOp,
   * and this MapOp is the root of the sub-tree. This method returns the private
   * Configuration stored for that MapOp. If that configuration is null,
   * it creates a new configuration and stores it in that "root" of the sub-tree,
   * and returns it.
   * 
   * @return
   */
  private Configuration getConf()
  {
    if (conf != null)
    {
      return conf;
    }

    RenderedImageMapOp currRenderedImageMapOp = this;
    MapOp parent = getParent();
    while (parent != null && (parent instanceof RenderedImageMapOp))
    {
      currRenderedImageMapOp = (RenderedImageMapOp)parent;
      Configuration c = currRenderedImageMapOp.conf;
      if (c != null)
      {
        return c;
      }
      parent = parent.getParent();
    }
    // No existing configuration was found for this subtree of
    // RenderedImageMapOps, so let's create a new configuration for
    // the "root" of that sub-tree.
    currRenderedImageMapOp.conf = createConfiguration();
    return currRenderedImageMapOp.conf;
  }
}
