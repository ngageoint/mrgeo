/*
 * Copyright (c) 2009-2010 by SPADAC Inc.  All rights reserved.
 */
package org.mrgeo.mapreduce;


/**
 * 
 */
public class CostSurfaceCalculator
{
//  public final static String MAX_COST = "maxCost";
//  public final static String PAST_END_MULTIPLIER = "pastEndMultiplier";
//  public final static String DESTINATION_POINTS = "desitnationPoints";
//  public final static String FRICTION = "friction";
//  
//  public static double _pastEndMultiplier = 1.0;
//  public static String _destinationPoints = null;
//
//  public static class MapClass extends
//      Mapper<LongWritable, BytesWritable, LongWritable, BytesWritable>
//  {
//    private double maxCost = -1;
//    private String friction;
//    
//    // Multiplier used to determine end point buffer for cost calculations
////    private double pastEndMultiplier = 1.0;
//    public static HashMap<Geometry,Double> _destinationPoints = null;
//
//    @Override
//    public void setup(Context context)
//    {
//      maxCost = Double.valueOf(context.getConfiguration().get(MAX_COST));
//      friction = context.getConfiguration().get(FRICTION);
//      
//      // Parse the destination points into Geometry objects
//      String values = (context.getConfiguration().get(DESTINATION_POINTS));
//      if(values != null)
//      {
//        _destinationPoints = CostSurfaceCalculator.parseDestinationPointsInput(values);
//      }
//    }
//
//    @Override
//    public void map(LongWritable key, BytesWritable value, Context context) throws IOException,
//        InterruptedException
//    {
//      Cluster c;
//      try
//      {
//
//        c = (Cluster) ObjectBytesConverter.toObject(value);
//        if (c == null)
//        {
//          throw new IOException("Didn't get a cluster");
//        }
//      }
//      catch (ClassNotFoundException e)
//      {
//        e.printStackTrace();
//        throw new IOException(e.getMessage());
//      }
//
//      Path cost = c.getBasePyramid();
//      TileManager manager = new TileManager(cost, CostDistanceCluster.class);
//      BigCostSurfaceCalculator csc = new BigCostSurfaceCalculator(manager, maxCost, _destinationPoints);
//      csc.setFriction(new Path(friction));
//      TreeMap<DirtyChange, Boolean> tmp = csc.processCluster(c);
//      
//      // Get processed dest pt list and write to context
//      HashMap<Geometry,Double> destPts = csc.getDestinationPointsMap();
//      if(destPts != null)
//      {
//        context.write(key, ObjectBytesConverter.toBytes(destPts));
//      }
//      
//      context.write(key, ObjectBytesConverter.toBytes(tmp));      
//    }
//  }
//
//  public static String FIRST_PASS = "firstPass";
//
//  public static String FRICTION_SURFACE = "frictionSurface";
//
//  public static void run(Path friction, Path cost, double maxCost, Progress progress,
//      JobListener jobListener
//      ) throws Exception
//  {
//    run(friction, cost, maxCost, progress, HadoopUtils.createConfiguration(), jobListener);
//  }
//  
//  public static void setDestinationPoints(String destinationPoints)
//  {
//    _destinationPoints = destinationPoints;
//  }
//  
//  public static void setPastEndMultiplier(double mult)
//  {
//    _pastEndMultiplier = mult;
//  }
//
//  @SuppressWarnings("unchecked")
//  public static void run(Path friction, Path cost, double maxCost, Progress progress,
//      Configuration configuration, JobListener jobListener) 
//          throws Exception
//  {
//    int maxClusters = configuration.getInt("mapred.map.tasks", 20);
//    OpImageRegistrar.registerMrGeoOps();
//
//    String input;
//    String output;
//
//    Runtime rt = Runtime.getRuntime();
//    System.out.println(String.format("memory: %d / %d free: %d", rt.totalMemory(), rt.maxMemory(),
//        rt.freeMemory()));
//    
//    FileSystem fs = HadoopUtils.getFileSystem(cost);
//    if (fs.exists(cost) == false)
//    {
//      MrsPyramidv1 frictionPyramid = MrsPyramidv1.loadPyramid(friction);
//      
//      MrsPyramidv1.createEmptyImage(cost, frictionPyramid.getBounds(), frictionPyramid.getImage(0).getWidth(), frictionPyramid.getImage(0)
//          .getHeight(), frictionPyramid.getTileSize(), MrsPyramidv1.DEFAULT_VALUE);
//    }
//
//    TileManager manager = new TileManager(cost, CostDistanceCluster.class);
//
//    int i = 0;
//    long changedValues = 1;
//
//    System.out.println("Finding best clusters.");
//    Collection<Cluster> clusters = manager.findBestClusters(maxClusters);
//    System.out.println("Exporting cluster image.");
//    System.out.flush();
//    // manager.exportClusterImage(); // for debugging..
//
//    System.out.println("  done.");
//    System.out.flush();
//
//    float p = 0;
//    float dp = 50;
//    
//    // Keep list of all destination points 
//    HashMap<Geometry,Double> destinationPoints = null;
//    if(_destinationPoints != null)
//    {
//      destinationPoints = parseDestinationPointsInput(_destinationPoints);
//    }
//
//    while (clusters.isEmpty() == false)
//    {
//      System.out.println("Creating Job conf.");
//
//      // this can cause memory problems.
////      // if it is fewer than two clusters it is faster to just run locally.
////      if (clusters.size() <= 2)
////      {
////        configuration.set("mapred.job.tracker", "local");
////      }
//      
//      Job job = new Job(configuration);
//      
//      String now = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss").format(new Date());
//      String jobName = "CostSurfaceCalculator" + now + "_" + UUID.randomUUID().toString();
//      job.setJobName(jobName);
//
//      Configuration conf = job.getConfiguration();
//
//      HadoopUtils.setJar(job);
//
//      System.out.println(String.format("Cluster count: %d", clusters.size()));
//
//      input = String.format(jobName + "/MapIn%04d.seq", i);
//
//      //FileSystem fs = HadoopUtils.getFileSystem(new Path(input));
//
//      // Add settings to configuration for map/reduce jobs 
//      conf.set(MAX_COST, String.format("%f", maxCost));
//      conf.set(FRICTION, friction.toString());
////      conf.set(PAST_END_MULTIPLIER, String.format("%f", _pastEndMultiplier));
//      if(_destinationPoints != null)
//      {
//        conf.set(DESTINATION_POINTS, _destinationPoints);
//      }
//
//      // the keys are ids that correspond to pixels
//      job.setOutputKeyClass(LongWritable.class);
//      // the values are costs represented as strings
//      job.setOutputValueClass(BytesWritable.class);
//
//      job.setMapperClass(MapClass.class);
//      job.setNumReduceTasks(0);
//
//      // TODO implement your own input format handler so each task gets 1 tile.
//      job.setInputFormatClass(SequenceFileInputFormat.class);
//      job.setOutputFormatClass(SequenceFileOutputFormat.class);
//
//      output = String.format(jobName + "/MapOut%04d.seq", i);
//
//      runJob(input, output, clusters, job, jobListener);
//
//      Path outputFp = new Path(output);
//
//      // a lame form of progress. We don't know how long this will take, so
//      // increase it by
//      // a smaller amount at each iteration. Progress will read something like:
//      // 0, 50, 75, 87, 93, ...
//      p += dp;
//      dp *= .5;
//      if (progress != null)
//      {
//        progress.set(p);
//      }
//
//      // changedValues =
//      // job.getCounters().getCounter(Reduce.Counters.CHANGED_VALUES);
//      System.out.println("Changed Values: " + changedValues);
//      // System.out.println("Visits: " +
//      // job.getCounters().getCounter(Reduce.Counters.VISITS));
//
//      if (fs.getFileStatus(outputFp).isDir() == false)
//      {
//        throw new IOException("Huh? output should be a directory");
//      }
//
//      LongWritable key = new LongWritable();
//      BytesWritable value = new BytesWritable();
//
//      FileStatus[] children = fs.listStatus(outputFp);
//      TreeMap<DirtyChange, Boolean> allChanges = new TreeMap<DirtyChange, Boolean>();
//      for (int j = 0; j < children.length; j++)
//      {
//        // ignore all the non-part files
//        if (!children[j].getPath().getName().startsWith("part"))
//        {
//          System.out.println("Ignoring: " + children[j].getPath().getName());
//          continue;
//        }
//        System.out.println("Processing: " + children[j].getPath().getName());
//        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(outputFp, children[j]
//            .getPath()), conf);
//        while (reader.next(key, value))
//        {
//          // Extract tile status and destination points list from completed map/reduce task
//          Object readValue = ObjectBytesConverter.toObject(value);
//          if(readValue instanceof TreeMap<?, ?>)
//          {
//            TreeMap<DirtyChange, Boolean> changes = (TreeMap<DirtyChange, Boolean>) readValue;
//            allChanges = BigCostSurfaceCalculator.combineChanges(changes, allChanges);
//          }
//          else if( readValue instanceof HashMap<?,?> && destinationPoints != null)
//          {
//            HashMap<Geometry,Double> destPtsRead = (HashMap<Geometry,Double>) readValue;
//            combineProcessedPoints(destinationPoints,destPtsRead);
//          }
//          else
//          {
//            System.out.println("Did not get a dirty change back. :(");
//          }
//        }
//        reader.close();
//      }
//      
//      // Update maxcost based on processed points list
//      // If all dest pts have valid values, pick max value and mult
//      // with pastendmult and set as new maxcost
//      if(destinationPoints != null)
//      {
//        boolean processed = true;
//        double maxValue = -1.0;
//        for(Geometry pt : destinationPoints.keySet())
//        {
//          double v = destinationPoints.get(pt);
//          if(v < 0)
//          {
//            processed = false;
//            break;
//          }
//
//          if(v > maxValue)
//          {
//            maxValue = v;
//          }
//        }
//
//        // If all destination points have cost values, use the top value
//        // to update the maxcost parameter
//        if(processed)
//        {
//          maxCost = _pastEndMultiplier * maxValue;    
//        }
//      }
//
//      manager.updateChanges(allChanges);
//      // for (DirtyChange dc : allChanges.keySet())
//      // {
//      // boolean v = allChanges.get(dc);
//      // System.out.println(dc.toString() + " " + String.valueOf(v));
//      // }
//      clusters = manager.findBestClusters(maxClusters);
//      for (Cluster c : clusters)
//      {
//        System.out.println(c.toString());
//      }
//      // manager.exportClusterImage(); // for debugging..
//
//      i++;
//    }
//  }
//  
//  /**
//   * The expected format is 'Place1','POINT(66.6701 34.041)';'Place2','POINT(66.25 34.75)';'Place3','POINT(64.9 31.35)'
//   * or 'POINT(66.6701 34.041)';'POINT(66.25 34.75)';'POINT(64.9 31.35)'
//   */
//  public static HashMap<Geometry,Double> parseDestinationPointsInput(String input)
//  {
//    HashMap<Geometry,Double> result = new HashMap<Geometry,Double>();
//    String[] splits = input.split(";");
//    for( String s : splits)
//    {
//      String line = s;
//      String[] lineSplits = line.split(",");
//      String point = lineSplits[lineSplits.length-1];
//      point = point.substring(1, point.length() -1);
//      WKTReader reader = new WKTReader();
//      try
//      {
//        Geometry pt = reader.read(point);
//        result.put(pt,-1.0);
//      }
//      catch (ParseException e)
//      {
//        System.out.println("Could not read destination points inlineCsv!");
//        e.printStackTrace();
//      }
//    }
//    return result;
//  }
//  
//  private static Geometry getKey(HashMap<Geometry,Double> destinationPoints, Geometry key)
//  {
//    for(Geometry k : destinationPoints.keySet())
//    {
//      if(key.toString().equals(k.toString()))
//      {
//        return k;
//      }
//    }
//    return null;
//  }
//  
//  /**
//   * Method to pick destination points with higher cost values.  This will
//   * update the master destination points list with the individual map/reduce
//   * results.
//   * @param destinationPoints The master destination points list
//   * @param destPtsRead The individual map/reduce destination points list
//   */
//  private static void combineProcessedPoints(HashMap<Geometry,Double> destinationPoints,HashMap<Geometry,Double> destPtsRead)
//  {
//    for(Geometry pt : destPtsRead.keySet())
//    {
//      Geometry key = getKey(destinationPoints,pt);
//      if(key != null)
//      {
//        if(destPtsRead.get(pt) > destinationPoints.get(key))
//        {
//          destinationPoints.put(key, destPtsRead.get(pt));
//        }      
//      }
//      else
//      {
//        destinationPoints.put(pt, destPtsRead.get(pt));
//      }
//    }
//  }
//
//  private static void runJob(String input, String output, Collection<Cluster> clusters, Job job,
//      JobListener jobListener)
//      throws IOException, JobFailedException, JobCancelledException
//  {
//    Path inputFp = new Path(input);
//    Configuration conf = job.getConfiguration();
//
//    FileSystem fs = inputFp.getFileSystem(conf);
//
//    if (fs.exists(inputFp))
//    {
//      fs.delete(inputFp, true);
//    }
//    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, inputFp, LongWritable.class,
//        BytesWritable.class);
//    int cn = 0;
//    for (Cluster c : clusters)
//    {
//      LongWritable key = new LongWritable(cn);
//      BytesWritable value = ObjectBytesConverter.toBytes(c);
//      System.out.println("Cost: " + c.getBasePyramid());
//      writer.append(key, value);
//      cn++;
//    }
//    writer.close();
//    
//    long length = fs.getFileStatus(inputFp).getLen();
//    System.out.printf("File Length: %d (split size: %d)\n", length, length / clusters.size());
//    FileInputFormat.setMaxInputSplitSize(job, length / clusters.size());
//    FileInputFormat.setInputPaths(job, input);
//
//    Path outputFp = new Path(output);
//    if (fs.exists(outputFp))
//    {
//      fs.delete(outputFp, true);
//    }
//
//    FileOutputFormat.setOutputPath(job, new Path(output));
//
//    MapReduceUtils.runJob(job, null, jobListener);
//  }

}
