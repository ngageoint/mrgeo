/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */

package org.mrgeo.rasterops;



/**
 * Handles very large cost surface calculations (10GB images should be feasible)
 * 
 * @author jason.surratt
 */

// TODO:  Needs to be converted to MrsImagePyramid v2
public class BigCostSurfaceCalculator
{
//  public class CostThread extends Thread
//  {
//    private Cluster cluster;
//    private IOException error;
//    private BigCostSurfaceCalculator parent;
//    private TreeMap<DirtyChange, Boolean> result;
//
//    public CostThread(BigCostSurfaceCalculator parent, Cluster cluster)
//    {
//      this.parent = parent;
//      this.cluster = cluster;
//    }
//
//    public TreeMap<DirtyChange, Boolean> getResult() throws IOException
//    {
//      if (error != null)
//      {
//        throw error;
//      }
//      return result;
//    }
//
//    public void run()
//    {
//      System.out.println("Thread started");
//      try
//      {
//        result = parent.processCluster(cluster);
//      }
//      catch (IOException e)
//      {
//        System.out.println("Got an IO Exception while processing cluster.");
//        for (TileInfo ti : cluster.getTileCopy())
//        {
//          System.out.println(" " + ti.toString());
//        }
//        error = e;
//      }
//      System.out.println("Thread complete");
//    }
//  }
//
//  /**
//   * Combine the dirty changes from two maps. If an entry is in both maps the
//   * true value takes precedence.
//   * 
//   * @param c1
//   * @param c2
//   * @return
//   */
//  public static TreeMap<DirtyChange, Boolean> combineChanges(TreeMap<DirtyChange, Boolean> c1,
//      TreeMap<DirtyChange, Boolean> c2)
//  {
//    TreeMap<DirtyChange, Boolean> result = (TreeMap<DirtyChange, Boolean>) c1.clone();
//
//    for (DirtyChange k : c2.keySet())
//    {
//      if (result.containsKey(k))
//      {
//        if (result.get(k) == false)
//        {
//          result.put(k, c2.get(k));
//        }
//      }
//      else
//      {
//        result.put(k, c2.get(k));
//      }
//    }
//
//    return result;
//  }
//
//  public static void main(String[] args) throws Exception
//  {
//    String friction = args[0];
//    String cost = args[1];
//    String xs = args[2];
//    String ys = args[3];
//
//    OpImageRegistrar.registerMrGeoOps();
//
//    long start = new Date().getTime();
//
//    Path costPath = new Path(cost);
//    MrsPyramidv1.delete(costPath);
//
//    FileSystem fs = HadoopUtils.getFileSystem(costPath);
//    MrsPyramidv1 frictionPyd = MrsPyramidv1.loadPyramid(new Path(friction));
//    MrsImagev1 frictionImage = frictionPyd.getImage(0);
//    if (fs.exists(costPath) == false)
//    {
//      MrsPyramidv1.createEmptyImage(costPath, frictionPyd.getBounds(), frictionImage.getWidth(), frictionImage
//          .getHeight(), frictionPyd.getTileSize(), MrsPyramidv1.DEFAULT_VALUE);
//    }
//    
//    TileManager manager = new TileManager(cost, CostDistanceCluster.class);
//    //MrsImage f = manager.getFriction().getImage(0);
//    int px = (int) frictionImage.convertToPixelX(Double.valueOf(xs));
//    int py = (int) frictionImage.convertToPixelY(Double.valueOf(ys));
//    System.out.printf("xs: %s px: %d, ys: %s py: %d\n", xs, px, ys, py);
//    manager.setRasterPixel(px, py, 0);
//    BigCostSurfaceCalculator bcsc = new BigCostSurfaceCalculator(manager);
//    bcsc.setFriction(new Path(friction));
//    bcsc.calculateCost();
//    System.out.println(String.format("Elapsed: %dms", new Date().getTime() - start));
//  }
//
//  private TileManager manager;
//  private MrsPyramidv1 frictionPyramid;
//
//  private int maxClusters = 9;
//  
//  private double maxCost = -1;
//  
//  //Multiplier used to determine end point buffer for cost calculations
////  private double pastEndMultiplier = 1.0;
//  private HashMap<Geometry,Double> destinationPoints = null;
//  
//  public BigCostSurfaceCalculator(TileManager manager)
//  {
//    this(manager, -1);
//  }
//  
//  public BigCostSurfaceCalculator(TileManager manager, double maxCost, HashMap<Geometry,Double> destPts)
//  {
//    this.destinationPoints = destPts;
//    this.manager = manager;
//    this.maxCost = maxCost;
//  }
//
//  public BigCostSurfaceCalculator(TileManager manager, double maxCost)
//  {
//    this.manager = manager;
//    this.maxCost = maxCost;
//  }
//  public void calculateCost() throws Exception
//  {
//    Collection<Cluster> clusters = manager.findBestClusters(maxClusters);
//    // manager.exportClusterImage(); // for debugging..
//
//    while (clusters.isEmpty() == false)
//    {
//      System.out.println(String.format("Cluster count: %d", clusters.size()));
//      TreeMap<DirtyChange, Boolean> changes = new TreeMap<DirtyChange, Boolean>();
//      LinkedList<CostThread> threads = new LinkedList<CostThread>();
//      for (Cluster c : clusters)
//      {
//        CostThread t = new CostThread(this, (Cluster) c.clone());
//        t.start();
//        threads.add(t);
//      }
//
//      for (CostThread t : threads)
//      {
//        try
//        {
//          t.join();
//          changes = combineChanges(changes, t.getResult());
//        }
//        catch (InterruptedException e)
//        {
//          e.printStackTrace();
//        }
//      }
//      manager.updateChanges(changes);
//      clusters = manager.findBestClusters(maxClusters);
//      // manager.exportClusterImage(); // for debugging..
//    }
//  }
//
//  public TreeMap<DirtyChange, Boolean> processCluster(Cluster cluster) throws IOException
//  {
//    Raster friction;
//    WritableRaster cost = null;
//
//    Runtime rt = Runtime.getRuntime();
//    System.gc();
//    System.out.println(String.format("memory: %.3fG / %.3fG", (rt.totalMemory() - rt
//        .freeMemory()) / 1e9, (rt.maxMemory()) / 1e9));
//
//    Rectangle r = cluster.getRectangle();
//    RenderedImage ri = PixelSizeMultiplierOpImage.create(frictionPyramid.getImage(0), null);
//    friction = ri.getData(cluster.getRectangle());
//
//    System.gc();
//
//    System.out.println(String.format("memory: %.3fG / %.3fG", (rt.totalMemory() - rt
//        .freeMemory()) / 1e9, (rt.maxMemory()) / 1e9));
//    System.gc();
//    System.out.println(String.format("memory: %.3fG / %.3fG", (rt.totalMemory() - rt
//        .freeMemory()) / 1e9, (rt.maxMemory()) / 1e9));
//
//    MrsPyramidv1 costPyramid = manager.getPyramid();
//    cost = costPyramid.getImage(0).getColorModel().createCompatibleWritableRaster(r.width,
//        r.height);
//    cost = cost.createWritableTranslatedChild(r.x, r.y);
//
//    costPyramid.getImage(0).copyData(cost);
//
//    // this will calculate a hash on the borders so we can check for changes
//    cluster.setBaseRaster(cost);
//    System.gc();
//
//    System.out.println(String.format("memory: %.3fG / %.3fG", (rt.totalMemory() - rt
//        .freeMemory()) / 1e9, (rt.maxMemory()) / 1e9));
//    CostSurfaceCalculator csc = new CostSurfaceCalculator();
//    csc.setFriction(friction);
//    csc.setMaxCost(maxCost);
//
//    long start = new Date().getTime();
//    try
//    {
//      csc.updateCostSurface(cost);
//    }
//    catch (OutOfMemoryError e)
//    {
//      e.printStackTrace();
//      System.out.println(String.format("memory: %.3fG / %.3fG", (rt.totalMemory() - rt
//          .freeMemory()) / 1e9, (rt.maxMemory()) / 1e9));
//      System.exit(-1);
//    }
//    
//    if(destinationPoints != null)
//    {
//      sampleCostRaster(costPyramid.getImage(0), destinationPoints);    
//    }
//    
//    long elapsed = new Date().getTime() - start;
//    System.out.println(String.format("updateCostSurface elapsed %dms", elapsed));
//
//    System.out.println(String.format("memory: %.3fG / %.3fG (%dms)", (rt.totalMemory() - rt
//        .freeMemory()) / 1e9, (rt.maxMemory()) / 1e9, new Date().getTime() - start));
//
//    friction = null;
//    csc = null;
//    System.out.println(String.format("memory: %.3fG / %.3fG (%dms)", (rt.totalMemory() - rt
//        .freeMemory()) / 1e9, (rt.maxMemory()) / 1e9, new Date().getTime() - start));
//    rt.gc();
//    System.out.println(String.format("memory: %.3fG / %.3fG (%dms)", (rt.totalMemory() - rt
//        .freeMemory()) / 1e9, (rt.maxMemory()) / 1e9, new Date().getTime() - start));
//
//    TreeMap<DirtyChange, Boolean> changes = new TreeMap<DirtyChange, Boolean>();
//    cluster.markIntraTilesClean(changes);
//    cluster.markAllNeighborsDirty(changes, cost);
//
//    elapsed = new Date().getTime() - start;
//    System.out.println(String.format("updateCostSurface elapsed %dms", elapsed));
//    System.out.println("Writing...");
//    costPyramid.getImage(0).setData(cost);
//
//    System.out.println(String.format("memory: %.3fG / %.3fG (%dms)", (rt.totalMemory() - rt
//        .freeMemory()) / 1e9, (rt.maxMemory()) / 1e9, new Date().getTime() - start));
//    rt.gc();
//    System.out.println(String.format("memory: %.3fG / %.3fG (%dms)", (rt.totalMemory() - rt
//        .freeMemory()) / 1e9, (rt.maxMemory()) / 1e9, new Date().getTime() - start));
//
//    return changes;
//  }
//
//  /**
//   * Method samples a list of points in (lon,lat) space from a raster.
//   * @param cost Raster to be sampled
//   * @param points List of points for sampling
//   */
//  public void sampleCostRaster(MrsImagev1 cost, HashMap<Geometry, Double> points)
//  {
//    for(Geometry pt : points.keySet())
//    {
//      // Destination point coordinate
//      Coordinate coord = pt.getCoordinate();
//      double x = coord.x;
//      double y = coord.y;
//
//      int pixelX = (int) cost.convertToPixelX(x);
//      int pixelY = (int) cost.convertToPixelY(y);
//      double pixelCost = cost.getSampleDouble(pixelX, pixelY, 0);
//      if(!Double.isNaN(pixelCost))
//      {
//        points.put(pt, pixelCost);
//      }
//    }
//  }
//  
//  public void setFriction(Path frictionFile) throws IOException
//  {
//    FileSystem fs = HadoopUtils.getFileSystem(frictionFile);
//    if (fs.exists(frictionFile))
//    {
//      frictionPyramid = MrsPyramidv1.loadPyramid(frictionFile);
//    }
//  }
//  
//  /**
//   * Retrieve the destination points to cost value map.  
//   * *** Only use after running BigCostSurfaceCalculator.processCluster ***
//   * @return HashMap<Geometry,Double> destination points to cost map
//   */
//  public HashMap<Geometry,Double> getDestinationPointsMap()
//  {
//    return destinationPoints;
//  }

}
