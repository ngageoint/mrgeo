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

import org.apache.hadoop.conf.Configuration;
import org.mrgeo.mapreduce.formats.TileClusterInfo;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.mapreduce.job.JobListener;
import org.mrgeo.progress.Progress;
import org.mrgeo.progress.ProgressHierarchy;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MapAlgebraExecutioner
{
  private static final Logger _log = LoggerFactory.getLogger(MapAlgebraExecutioner.class);
  private JobListener jobListener = null;
  private static ExecutorService executorSvc = null;
  private Vector<Future<RunnableMapOp>> futures = new Vector<Future<RunnableMapOp>>();

  private class RunnableMapOp implements Runnable
  {
    Throwable exception = null;
    MapOp _op;
    ProgressHierarchy _progress;

    RunnableMapOp(Configuration conf, MapOp op, ProgressHierarchy progress)
    {
      _op = op;
      _progress = progress;
    }

    @Override
    public void run()
    {
      try
      {
        // Set up progress for execute listeners if there are any
        Progress opProgress = _progress;
        List<MapOp> executeListeners = _op.getExecuteListeners();
        if (_progress != null)
        {
          _progress.starting();
        }
 
        // Run the operation itself
        if (opProgress != null)
        {
          opProgress.starting();
        }
        if (_op instanceof OutputProducer)
        {
          ((OutputProducer)_op).resolveOutputName();
        }
        _op.build(opProgress);
        if (opProgress != null)
        {
          opProgress.complete();
        }
        
        // Build execute listeners if there are any
        if (executeListeners != null)
        {
          for (MapOp listener : executeListeners)
          {
            if (listener != null)
            {
              ProgressHierarchy listenerProgress = new ProgressHierarchy();
              if (listener instanceof OutputProducer)
              {
                ((OutputProducer) listener).resolveOutputName();
              }
              listener.build(listenerProgress);
            }
          }
        }
        if (_progress != null)
        {
          _progress.complete();
        }
      }
      catch (Throwable t)
      {
        t.printStackTrace();
        exception = t;
      }
    }
  }

  MapOp _root;
  String output = null;

  public void setOutputName(String p)
  {
    output = p;
  }

  public void setRoot(MapOp root)
  {
    _root = root;
  }

  public void setJobListener(JobListener jl)
  {
    jobListener = jl;
  }

  public void cancel()
  {
    synchronized (futures)
    {
      for (Future<RunnableMapOp> future : futures)
      {
        // cancel (true - may interrupt the job if running)
        future.cancel(true);
      }
    }
  }

  public void execute(Configuration conf, Progress p) throws JobFailedException,
  JobCancelledException
  {
    // TODO:  Change the default here to TRUE
    execute(conf, p, false);
  }

  public void execute(Configuration conf, Progress progress, boolean buildPyramid) throws 
  JobFailedException, JobCancelledException
  {
    try
    {
      if (_root == null)
      {
        throw new IllegalArgumentException("You must specify a root node.");
      }
      if (!(_root instanceof OutputProducer))
      {
        throw new IllegalArgumentException("The last operation in the map algebra must produce output");
      }
      ProgressHierarchy ph = new ProgressHierarchy(progress);
      ph.createChild(1f);
      ph.createChild(4f);
      ph.createChild(2f);
      if (Thread.currentThread().isInterrupted())
      {
        throw new InterruptedException();
      }
      TileClusterInfo tileClusterInfo = calculateTileClusterInfo(_root);
      MapAlgebraExecutioner.setOverallTileClusterInfo(_root, tileClusterInfo);
      executeChildren(conf, _root, ph.getChild(0));
      // If the root is deferred, then at this point it has been "prepared"
      // but not built. So we need to build it to get the final result.
      ((OutputProducer)_root).setOutputName(output);
      _root.build(ph.getChild(1));
      _root.postBuild(ph.getChild(2), buildPyramid);
    }
    catch (JobFailedException e)
    {
      // job interrupted
      cancel();
      throw (e);
    }
    catch (JobCancelledException e)
    {
      // job interrupted
      cancel();
      throw (e);
    }
    catch (InterruptedException e)
    {
      // job interrupted
      cancel();
      throw new JobCancelledException(e.getMessage());
    }
    catch (Exception e)
    {
      cancel();
      e.printStackTrace();
      throw new JobFailedException(e.getMessage());
    }
    finally
    {
      // Free up unneeded memory in the MapOp tree we just executed and saved
      _log.info("Clearing memory from the map op tree");
      _root.clear();
      try
      {
        _log.info("Cleaning temp files from the map op tree");
        cleanupMapOp(_root);
      }
      catch (IOException e)
      {
        _log.error("Failure while deleting temporary resources", e);
      }
    }
  }

  void executeChildren(final Configuration conf, final MapOp mapOp,
      final ProgressHierarchy ph) throws JobFailedException,
      InterruptedException, ExecutionException
  {
    // Set up all of the progress hierarchy we need ahead of time
    ProgressHierarchy phParent = null;
    ProgressHierarchy phChildren = null;
    if (ph != null)
    {
      ph.starting();
      phParent = ph.createChild(1f);
      phChildren = ph.createChild(1f);
      for (int i=0; i < mapOp.getInputs().size(); i++)
      {
        ProgressHierarchy phChild = phChildren.createChild(1f);
        phChild.createChild(1f);
        phChild.createChild(2f);
      }
    }
    mapOp.setJobListener(jobListener);
    // Store a list of the children that need to be built. After we've traversed
    // all the children, they are built in parallel.
    Vector<RunnableMapOp> v = new Vector<RunnableMapOp>();
    int i = 0;
    for (MapOp child : mapOp.getInputs())
    {
      ProgressHierarchy pc1 = null;
      ProgressHierarchy pc2 = null;
      if (ph != null)
      {
        pc1 = phChildren.getChild(i).getChild(0);
        pc2 = phChildren.getChild(i).getChild(1);
      }
      executeChildren(conf, child, pc2);
      // The build() call to the MapOp must be invoked while processing it's
      // parent because at the child level, there is not enough context
      // to decide if build() should be called.
      if (!(child instanceof DeferredExecutor) || !(mapOp instanceof DeferredExecutor))
      {
        v.add(new RunnableMapOp(conf, child, pc1));
      }
      // see if the child has any execute listeners (like a save(...)), if so, make sure we run
      // it here, otherwise the listeners are quietly ignored
      else if (child.getExecuteListeners() != null && child.getExecuteListeners().size() > 0)
      {
        v.add(new RunnableMapOp(conf, child, pc1));
      }
      else
      {
        // If both the child and the parent are deferred executors, but the child
        // is a different type of deferred executor, then we need to build the
        // child since they can't built together.
        DeferredExecutor deferredChild = (DeferredExecutor) child;
        DeferredExecutor deferredMapOp = (DeferredExecutor) mapOp;
        if (!deferredChild.getOperationId().equals(deferredMapOp.getOperationId()))
        {
          v.add(new RunnableMapOp(conf, child, pc1));
        }
      }
      i++;
    }
    // If there are children to be built, do so in parallel
    if (v.size() != 0)
    {
      runThreads(conf, v);
    }


    // The prepare() is called at the level being recursed because there is
    // nothing more to know than whether or not it is a deferred map op.
    if (mapOp instanceof DeferredExecutor)
    {
      try
      {
        ((DeferredExecutor)mapOp).prepare(phParent);
      }
      catch (IOException e)
      {
        _log.error("Failure running map algebra", e);
        throw new JobFailedException(e.getMessage());
      }
    }
    if (ph != null)
    {
      ph.complete();
    }
  }

  private static void cleanupMapOp(final MapOp mapOp) throws IOException
  {
    // Cleanup the entire tree of map ops
    for (MapOp child : mapOp.getInputs())
    {
      cleanupMapOp(child);
    }
    mapOp.cleanup();
  }

  private void addToFutures(Future<RunnableMapOp> f)
  {
    synchronized (futures)
    {
      futures.add(f);
    }
  }

  private void runThreads(Configuration conf, Vector<? extends RunnableMapOp> threads)
      throws JobFailedException, InterruptedException, ExecutionException
      {
    if (Thread.currentThread().isInterrupted())
    {
      throw new InterruptedException();
    }

    Vector<Future<RunnableMapOp>> futureList = new Vector<Future<RunnableMapOp>>();
    for (RunnableMapOp r : threads)
    {
      Future<RunnableMapOp> future = submit(conf, r);
      addToFutures(future);
      futureList.add(future);
    }

    // wait for all tasks to complete before continuing
    for (Future<RunnableMapOp> f : futureList)
    {
      f.get();
    }

    Throwable firstProblem = null;
    for (RunnableMapOp thread : threads)
    {
      if (thread.exception != null)
      {
        if (firstProblem == null)
        {
          firstProblem = thread.exception;
        }
        thread.exception.printStackTrace();
      }
    }

    if (firstProblem != null)
    {
      throw new JobFailedException(firstProblem.getMessage());
    }
      }

  private synchronized Future<RunnableMapOp> submit(Configuration conf, RunnableMapOp r)
  {
    if (executorSvc == null)
    {
      // we don't want to overload Hadoop w/ lots of job requests. 10 at a time
      // should be plenty to maximize use of the system. - TODO: move to a
      // config?
      // if we're using the local job tracker, we only want 1 thread, because
      // many versions of the
      // localJobTracker class are NOT threadsafe.

      if (HadoopUtils.isLocal(conf))
      {
        executorSvc = Executors.newFixedThreadPool(1);
      }
      else
      {
        executorSvc = Executors.newFixedThreadPool(10);
      }
    }
    @SuppressWarnings("unchecked")
    Future<RunnableMapOp> future = (Future<RunnableMapOp>) executorSvc.submit(r);
    return future;
  }

  /**
   * Calculate the input pyramid paths from the entire tree this image. This should _not_ call
   * getOutput() on its input MapOps.
   * 
   * @param mapOp
   * @param inputPyramids
   * @return
   * @throws IOException
   */
  public static void calculateInputs(MapOp mapOp, Set<String> inputPyramids)
  {
    if (mapOp instanceof InputsCalculator)
    {
      inputPyramids.addAll(((InputsCalculator)mapOp).calculateInputs());
    }
    else
    {
      for (final MapOp input : mapOp.getInputs())
      {
        calculateInputs(input, inputPyramids);
      }
    }
  }

  /**
   * Calculate the combined bounds of all the inputs, which gives us the total bounds of this image.
   * This should _not_ call getOutput() on its input MapOps.
   * 
   * @param mapOp
   * @return
   * @throws IOException
   */
  public static Bounds calculateBounds(MapOp mapOp) throws IOException
  {
    if (mapOp instanceof BoundsCalculator)
    {
      return ((BoundsCalculator)mapOp).calculateBounds();
    }
    else
    {
      final Bounds b = new Bounds();
      for (final MapOp input : mapOp.getInputs())
      {
        if (input != null)
        {
          b.expand(calculateBounds(input));
        }
      }
      return b;
    }
  }

  /**
   * Calculate the maximum (most detailed) zoom level of all the inputs below the
   * mapOp passed in. This should _not_ call getOutput() on its input MapOps.
   * 
   * @param mapOp
   * @return
   * @throws IOException
   */
  public static int calculateMaximumZoomlevel(MapOp mapOp) throws IOException
  {
    if (mapOp instanceof MaximumZoomLevelCalculator)
    {
      return ((MaximumZoomLevelCalculator)mapOp).calculateMaximumZoomlevel();
    }
    else
    {
      int zoom = 0;
      for (final MapOp input : mapOp.getInputs())
      {
        final int z = calculateMaximumZoomlevel(input);
        if (z > zoom)
        {
          zoom = z;
        }
      }
      return zoom;
    }
  }

  /**
   * Calculates the combined neighborhood of raster tiles to input.
   * @throws IOException 
   */
  public static TileClusterInfo calculateTileClusterInfo(MapOp mapOp) throws IOException
  {
    if (mapOp instanceof TileClusterInfoCalculator)
    {
      return ((TileClusterInfoCalculator)mapOp).calculateTileClusterInfo();
    }
    else
    {
      final TileClusterInfo tileClusterInfo = new TileClusterInfo();
      if (mapOp.getInputs() != null)
      {
        for (final MapOp input : mapOp.getInputs())
        {
          if (input != null)
          {
            tileClusterInfo.expand(calculateTileClusterInfo(input));
          }
        }
      }
      return tileClusterInfo;
    }
  }

  /**
   * Calculate the tile size of all the inputs. The tile size is expected to be the same for all
   * inputs. This should _not_ call getOutput() on its input MapOps.
   * 
   * @param mapOp
   * @return
   * @throws IOException
   */
  public static int calculateTileSize(MapOp mapOp) throws IOException
  {
    // Return the first tile size greater than 0. It is assumed at this point
    // that tile sizes of all rasters will be the same.
    if (mapOp instanceof TileSizeCalculator)
    {
      return ((TileSizeCalculator)mapOp).calculateTileSize();
    }
    else
    {
      for (final MapOp input : mapOp.getInputs())
      {
        int tileSize = calculateTileSize(input);
        if (tileSize > 0)
        {
          return tileSize;
        }
      }
    }
    return 0;
  }

  /**
   * After the tile cluster info has been computed across the entire MapOp tree, the results are set
   * using this method. MapOp's that require neighborhood tiles to do their job should store the
   * passed tileClusterInfo for use during their execution. The base MapOp implementation of this
   * function does not store the tile cluster info, it only passes it along to its children.
   * 
   * @param tileClusterInfo
   */
  public static void setOverallTileClusterInfo(final MapOp mapOp,
      final TileClusterInfo tileClusterInfo)
  {
    if (mapOp instanceof TileClusterInfoConsumer)
    {
      ((TileClusterInfoConsumer)mapOp).setOverallTileClusterInfo(tileClusterInfo);
    }
    // Recurse through the children
    if (mapOp.getInputs() != null)
    {
      for (final MapOp input : mapOp.getInputs())
      {
        setOverallTileClusterInfo(input, tileClusterInfo);
      }
    }
  }
}
