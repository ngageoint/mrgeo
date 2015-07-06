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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.mapreduce.job.JobListener;
import org.mrgeo.progress.Progress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for map algebra functions. When new map algebra
 * functions are created, they should not extend this class directly,
 * but instead, they should extend one of the direct sub-classes of
 * MapOp (i.e. RasterMapOp, VectorMapOp, ResourceMapOp, ProcedureMapOp)
 * based on the type of data it produces (if any). If no data is
 * produced, it should extend ProcedureMapOp.
 * 
 * Note also that there are interfaces that developers should consider
 * implementing when they write new MapOps. Those interfaces are in
 * the org.mrgeo.mapreduce package and include BoundsCalculator,
 * InputsCalculator, MaximumZoomLevelCalculator, TileClusterInfoCalculator,
 * TileClusterInfoConsumer, TileSizeCalculator and DeferredExecutor.
 * 
 * Sub-classes should implement DeferredExecutor if operations can be
 * chained together (parents and children) and built all at one time.
 * Many MrGeo raster map op's work this way because they are based on
 * JAI functionality, and those operations are strung together into a
 * single chain of JAI operations and processed together.
 */
public abstract class MapOp implements Cloneable
{
  private static final Logger log = LoggerFactory.getLogger(MapOp.class);
  protected ArrayList<MapOp> _inputs = new ArrayList<MapOp>();
  protected ArrayList<MapOp> executeListeners = new ArrayList<MapOp>();
  // TODO: The following remains to support vector data which has not yet been
  // transitioned to use data providers. It's still HDFS-specific. It should
  // eventually be migrated into tmpResources.
  private final HashSet<String> tmpPaths = new HashSet<String>();
  private final HashSet<String> tmpResources = new HashSet<String>();
  private Configuration defaultConf;
  private Properties providerProperties;
  private String protectionLevel;
  protected JobListener jobListener = null;

  private MapOp parent = null;
  private String functionName;

  public static String[] register()
  {
    return null;
  }

  public void setFunctionName(final String functionName)
  {
    this.functionName = functionName;
  }

  public String getFunctionName()
  {
    return functionName;
  }

  /**
   * Helper function for sub-classes to use within their implementation of the
   * processChildren() method. It parses a double number value from the node
   * passed in if possible, otherwise throws an IllegalArgumentException containing
   * the error string referencing paramName (e.g. "You must specify a valid
   * number for [paramName]").
   * 
   * @param n
   * @param paramName
   * @param parser
   * @return
   */
  protected static double parseChildDouble(final ParserNode n, final String paramName, final ParserAdapter parser)
  {
    try
    {
      Object o = parser.evaluate(n);
      if (o instanceof Number)
      {
        return ((Number)o).doubleValue();
      }
      else if (o instanceof String)
      {
        try
        {
          return Double.parseDouble((String)o);
        }
        catch(NumberFormatException e)
        {
          throw new IllegalArgumentException("You must specify a valid number for " + paramName
              + ". (try putting the value in double quotes)");
        }
      }
    }
    catch (ParserException e)
    {
      throw new IllegalArgumentException("You must specify a valid number for " + paramName
          + ". (try putting the value in double quotes)", e);
    }
    
    throw new IllegalArgumentException("You must specify a valid number for " + paramName
        + ". (try putting the value in double quotes)");
  }

  protected static boolean isChildDouble(final ParserNode n, final ParserAdapter parser)
  {
    try
    {
      Object o = parser.evaluate(n);
      if (o instanceof Number)
      {
        return true;
      }
      else if (o instanceof String)
      {
        try
        {
          Double.parseDouble((String)o);
          return true;
        }
        catch(NumberFormatException nfe)
        {
        }
      }
    }
    catch (ParserException e)
    {
    }
    return false;
  }

  /**
   * Helper function for sub-classes to use within their implementation of the
   * processChildren() method.
   * 
   * @param n
   * @param paramName
   * @param parser
   * @return
   */
  protected static int parseChildInt(final ParserNode n, final String paramName, final ParserAdapter parser)
  {
    try
    {
      Object o = parser.evaluate(n);
      if (o instanceof Number)
      {
        return Math.round(((Number)o).floatValue());
      }
      else if (o instanceof String)
      {
        try
        {
          return Integer.parseInt((String)o);
        }
        catch(NumberFormatException e)
        {
          throw new IllegalArgumentException("You must specify a valid number for " + paramName
              + ". (try putting the value in double quotes)");
        }
      }
    }
    catch (ParserException e)
    {
      throw new IllegalArgumentException("You must specify a valid number for " + paramName
          + ". (try putting the value in double quotes)", e);
    }
    
    throw new IllegalArgumentException("You must specify a valid number for " + paramName
        + ". (try putting the value in double quotes)");
  }

  protected static boolean isChildInt(final ParserNode n, final ParserAdapter parser)
  {
    try
    {
      Object o = parser.evaluate(n);
      if (o instanceof Number)
      {
        return true;
      }
      else if (o instanceof String)
      {
        try
        {
          Integer.parseInt((String)o);
          return true;
        }
        catch(NumberFormatException nfe)
        {
        }
      }
    }
    catch (ParserException e)
    {
    }
    return false;
  }

  protected static String parseChildString(final ParserNode n, final String paramName, final ParserAdapter parser)
  {
    
    try
    {
      return parseChildStringRaw(n, paramName, parser);
    }
    catch (ParserException e)
    {
      throw new IllegalArgumentException("You must specify a valid string for " + paramName
          + ". (try putting the value in double quotes)", e);
    }
  }

  protected static String parseChildStringRaw(final ParserNode n, final String paramName, final ParserAdapter parser) throws ParserException
  {
    
    Object o = parser.evaluate(n);
    if (o != null)
    {
      return o.toString();
    }
    throw new ParserException("You must specify a valid string for " + paramName
        + ". (try putting the value in double quotes)");
  }

  /**
   * The map algebra parser will call this method once for each entry returned
   * by the processChildren() method. Between this map op returning from
   * processChildren(), and the parser invoking this method, each of those
   * child nodes will have been resolved into MapOp's by the parser. The
   * parser also calls addInput() in the same order as the nodes were returned
   * from processChildren().
   * 
   * For example, suppose a map op returns two nodes from getChildren(), representing
   * raster input 1 and raster input 2. When the parser invokes addInput(), it will
   * first invoke it with the map op for raster input 1, and then invoke it again
   * with the map op for raster input 2.
   * 
   * If there are any problems with the resolved map ops (for example they are
   * an unexpected type), then the sub-classes should throw an IllegalArgumentException
   * with a description of the problem.
   * 
   * @param n
   * @throws IllegalArgumentException
   */
  public abstract void addInput(MapOp n) throws IllegalArgumentException;

  /**
   * Sub-classes that create temporary files or directories in HDFS should
   * pass those paths to this method so they can be deleted after the map
   * algebra executioner finishes running. This method will be eliminated
   * once all vestiges of HDFS-specific code is eliminated from map
   * algebra functions.
   * 
   * @deprecated Use addTempResource instead
   * @param p The name of the temporary resource
   */
//  public void addTempFile(final Path p)
//  {
//    tmpPaths.add(p.toString());
//  }

  /**
   * Sub-classes that create temporary files or directories in HDFS should
   * pass those paths to this method so they can be deleted after the map
   * algebra executioner finishes running. This method will be eliminated
   * once all vestiges of HDFS-specific code is eliminated from map
   * algebra functions.
   * 
   * @deprecated Use addTempResource instead
   * @param p The name of the temporary resource
   */
  public void addTempFile(final String p)
  {
    tmpPaths.add(p);
  }

  /**
   * Sub-classes that create temporary resources should pass those resources
   * to this method. After the map algebra executioner finishes running, it
   * will call the cleanup() method on each of the map ops to remove these
   * temporary resources.
   * 
   * @param resource The name of the temporary resource
   */
  public void addTempResource(final String resource)
  {
    tmpResources.add(resource);
  }

  /**
   * Cleanup any temporary resources that may have been created during processing.
   * Temporary resources must be passed to addTempResource() in order to be
   * cleaned up.
   */
  public void cleanup() throws IOException
  {
    for (final String resourceName : tmpResources)
    {
      DataProviderFactory.delete(resourceName, getProviderProperties());
    }
    for (final String p : tmpPaths)
    {
      HadoopFileUtils.delete(p);
    }
  }

  public void clear()
  {
//    _inputs.clear();
//    if (_output instanceof RenderedOp)
//    {
//      final RenderedOp rop = ((RenderedOp) _output);
//      final PlanarImage theImage = rop.getCurrentRendering();
//      // This is a workaround for a memory problem that was coming up when the
//      // ApplyPdfsMapOp ran. The individual map ops for each of the factors processed
//      // each had a TileInfo cache, and when the image bounds were large, that TileInfo
//      // cache became huge in each of the factors. Memory would be used up after about
//      // 12 factors or so. The ApplyPdfsMapOp now invokes this method after processing
//      // each factor to free up unneeded memory as it goes.
//      if ((theImage != null) && (TileCacheOpImage.class.isAssignableFrom(theImage.getClass())))
//      {
//        final TileCacheOpImage tc = (TileCacheOpImage) theImage;
//        tc.cleanup();
//      }
//    }
  }

  /**
   * Returns a clone of this MapOp and all its children. This will not include any intermediate
   * results or temporary variables involved in computation.
   */
  @Override
  public MapOp clone()
  {
    try
    {
      final MapOp result = (MapOp) super.clone();

      if (_inputs != null)
      {
        result._inputs = new ArrayList<MapOp>();
        for (final MapOp mo : _inputs)
        {
          result._inputs.add(mo.clone());
        }
      }
      else
      {
        result._inputs = null;
      }

      return result;
    }
    catch (final CloneNotSupportedException e)
    {
      log.error(e.getMessage());
      throw new Error("Error cloning MapOp, shouldn't be here.");
    }
  }

  /**
   * Most map ops will not override this method. It is called by classes that execute map op chain
   * to decide whether to delete the output directory so the results of running the map op chain
   * completely replace the output that is already there.
   * 
   * Map ops should only override this if they need the existing output to remain intact so they can
   * append or replace new output.
   * 
   * @return
   */
  @SuppressWarnings("static-method")
  public boolean deleteExistingOutput()
  {
    return true;
  }

  /**
   * Sub-classes must provide an implementation for this method in order to actually
   * perform their processing. The map algebra executioner invokes this method at the
   * appropriate time. Keep in mind that the build() method may not be invoked for
   * every map op when they implement the DeferredExecutor interface.
   * See the documentation for MapAlgebraExecutioner for more information.
   * 
   * @param p
   * @throws IOException
   * @throws JobFailedException
   * @throws JobCancelledException
   */
  public abstract void build(final Progress p)
      throws IOException,
      JobFailedException, JobCancelledException;

  /**
   * This method is invoked on the root map op after all the map ops in the tree
   * have been built.
   * 
   * @param p
   * @param buildPyramid
   * @throws IOException
   * @throws JobFailedException
   * @throws JobCancelledException
   */
  public abstract void postBuild(final Progress p, boolean buildPyramid)
      throws IOException, JobFailedException, JobCancelledException;

  /**
   * Returns the root map op from the tree.
   * 
   * @return
   */
  public MapOp findRoot()
  {
    if (parent == null)
    {
      return this;
    }

    return parent.findRoot();
  }

  /**
   * Sets the configuration to use for cloning configurations when running
   * Hadoop jobs. This method is invoked by the MapAlgebraParser on the root
   * MapOp for the MapOp tree. MapOps call createConfiguration() in order to
   * obtain a Hadoop Configuration they can use for running a Hadoop job.
   * That will use the default configuration from the root of the MapOp tree
   * and clone it, returning the result. This prevents different MapOps in
   * the tree from modifying the same Configuration and "polluting" each others'
   * jobs.
   * @param conf
   */
  public void setDefaultConfiguration(final Configuration conf)
  {
    this.defaultConf = conf;
  }

  /**
   * Return a cloned instance of the default Hadoop Configuration from the root
   * of the MapOp tree. See setDefaultConfiguration for more information.
   * 
   * @return
   */
  public Configuration createConfiguration()
  {
    Configuration conf = this.defaultConf;
    MapOp parent = getParent();
    while (conf == null && parent != null)
    {
      conf = parent.defaultConf;
      parent = parent.getParent();
    }
    return conf;
  }

  /**
   * Return the provider properties that the map op was configured with.
   * @return
   */
  public Properties getProviderProperties()
  {
    return providerProperties;
  }

  public void setProtectionLevel(final String protectionLevel)
  {
    this.protectionLevel = protectionLevel;
  }

  public String getProtectionLevel()
  {
    return protectionLevel;
  }

  /**
   * Returns a read-only version of the inputs vector. Exposed for the executor. Some MapOp's may
   * not store their inputs as a list so it would not be safe to modify this.
   * 
   * @return
   */
  public List<MapOp> getInputs()
  {
    return Collections.unmodifiableList(_inputs);
  }

  /**
   * Returns this map op's parent map op or null if this is the root map op.
   * @return
   */
  public MapOp getParent()
  {
    return parent;
  }

  /**
   * Returns true if the path passed in was added to the temp file list via an
   * earlier call to addTempFile().
   * 
   * @param p
   */
//  public boolean isTempFile(final Path p)
//  {
//    return p != null ? isTempFile(p.toString()) : false;
//  }

  /**
   * Returns true if this resource was added to the list of temporary resources
   * via an earlier call to addTempResource().
   * 
   * @param p
   * @return
   */
  public boolean isTempFile(final String p)
  {
    return p != null ? (tmpResources.contains(p) || tmpPaths.contains(p)) : false;
  }

  /**
   * This method is called while the map algebra is being parsed. Sub-classes should
   * override this method in order to perform their own parsing of their children
   * (e.g. the arguments passed to the map op). This is useful when some arguments
   * are constants which need to be stored for use later when prepare() and/or
   * build() is invoked.
   * 
   * If there are any problems with the children, then the sub-class should throw
   * an IllegalArgumentException containing a description of the problem. This error
   * will be reported, and it will prevent the map algebra executioner from running.
   * 
   * The return Vector should contain each of the Nodes contained in the children
   * parameter that require further resolution (e.g. are not constant values like
   * strings or numbers). For constant values, the map op should parse those in
   * this method and store the result in a member variable so the values are
   * available when prepare() and build() are called.
   * 
   * @param children
   * @param parser
   * @return
   */
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children,
      final ParserAdapter parser)
  {
    return children;
  }

  public void setProviderProperties(final Properties props)
  {
    providerProperties = props;
  }

  /**
   * Replaces a specific entry in the inputs of this map op with a different
   * input (map op).
   * 
   * @param i
   * @param replacer
   */
  public void setInput(final int i, final MapOp replacer)
  {
    _inputs.set(i, replacer);
  }

  /**
   * Sets a listener to be invoked immediately after this map op's build()
   * has executed.
   * 
   * @param jl
   */
  public void setJobListener(final JobListener jl)
  {
    jobListener = jl;
  }

  public void setParent(final MapOp mapop)
  {
    parent = mapop;
  }

  /**
   * The toString function is used in calculated equivalency between two operations (much faster for
   * comparing trees when optimizing). Be sure toString is both reasonably readable and mostly
   * unique.
   */
  @Override
  public String toString()
  {
    assert (false);
    return null;
  }

  public String printTree()
  {
    StringBuilder builder = new StringBuilder();
    walkAndPrintTree(this, 0, builder);

    return builder.toString();
  }

  private void walkAndPrintTree(MapOp op, int level, StringBuilder builder)
  {
    for (int i = 0; i < level; i++)
    {
      builder.append("  ");
    }
    builder.append(op.toString());
    builder.append('\n');

    for (MapOp input : op._inputs)
    {
      walkAndPrintTree(input, level + 1, builder);
    }
  }
  /**
   * Adds an execute listener to this MapOp. The MapAlgebraExecutioner will take care of
   * calling each of the executeListeners after this MapOp executes. The map algebra
   * executioner takes care of invoking the listener.
   * 
   * @param listener
   */
  public void addExecuteListener(final MapOp listener)
  {
    executeListeners.add(listener);
  }

  /**
   * Returns a read-only version of the execute listeners vector. Exposed for the executor.
   * These listeners are invoked by the map algebra executioner immediately after this
   * map op's build() completes.
   * 
   * @return
   */
  public List<MapOp> getExecuteListeners()
  {
    return Collections.unmodifiableList(executeListeners);
  }
}
