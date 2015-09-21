package org.mrgeo.mapalgebra;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.util.ToolRunner;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapalgebra.old.*;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.opimage.MrsPyramidDescriptor;
import org.mrgeo.progress.Progress;
import org.mrgeo.rasterops.CostDistanceDriver;
import org.mrgeo.utils.Bounds;

public class CostDistanceMapOp extends RasterMapOpHadoop
  implements InputsCalculator, BoundsCalculator, TileSizeCalculator, MaximumZoomLevelCalculator
{
  double maxCost = -1;

  int zoomLevel = -1;
  private int tileSize = -1;
  
  @Override
  public void addInput(MapOpHadoop n) throws IllegalArgumentException
  {
    if (_inputs.size() == 1)
    {
      // The friction surface is now being added - make sure it is a raster
      if (!(n instanceof RasterMapOpHadoop))
      {
        throw new IllegalArgumentException("The second argument to CostDistance is the friction surface which must be an ingested image");
      }
      if (((RasterMapOpHadoop)n).getOutputName() == null)
      {
        throw new IllegalArgumentException("Invalid raster input to CostDistance. The friction raster has no output name.");
      }
    }
    _inputs.add(n);
  }


  @Override
  public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
  {
    if (p != null)
    {
      p.starting();
    }
    
    MapOpHadoop inlineCsvMapOp = _inputs.get(0);
    
    // Extract source point from vector input
    assert(inlineCsvMapOp instanceof InlineCsvMapOp);
     InlineCsvInputFormatDescriptor ifd =  
             (InlineCsvInputFormatDescriptor) ((InlineCsvMapOp)inlineCsvMapOp).getVectorOutput();
     String sourcePoints = ifd.getValues();

    // Get friction input
    RasterMapOpHadoop friction = (RasterMapOpHadoop) _inputs.get(1);

    if (zoomLevel < 0)
    {
      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(friction.getOutputName(), AccessMode.READ,
          getProviderProperties());
      MrsImagePyramidMetadata metadata = dp.getMetadataReader().read();
      zoomLevel = metadata.getMaxZoomLevel();
    }

    try {
  	  String driverArgs[] = new String[] {
  			  					"-inputpyramid", friction.getOutputName(),
  			  					"-inputzoomlevel", "" + zoomLevel,
  			  					"-outputpyramid", _outputName,
  			  					"-sourcepoints", sourcePoints,
  			  					"-maxcost", String.valueOf(maxCost)};
  	  
//      int statusJob = ToolRunner.run(getConf(), 
//        new CostDistanceDriver(), 
//        driverArgs);
      if (ToolRunner.run(createConfiguration(), new CostDistanceDriver(), driverArgs) < 0)
      {
        throw new JobFailedException("CostDistance job failed, see Hadoop logs for more information");
      }
  	
    } catch (Exception e) {
      e.printStackTrace();
    	throw new JobFailedException(e.getMessage());
    }
    
    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(_outputName,
        AccessMode.READ, getProviderProperties());
    _output = MrsPyramidDescriptor.create(dp);
    
    if (p != null)
    {
      p.complete();
    }
  }

  @Override
  public int calculateTileSize() throws IOException
  {
    if (tileSize < 0)
    {
      RasterMapOpHadoop friction = (RasterMapOpHadoop) _inputs.get(1);
      
      assert(friction.getOutputName() != null);
      
      MrsImagePyramid pyramid = MrsImagePyramid.open(friction.getOutputName(),
          getProviderProperties());
      tileSize = pyramid.getTileSize();
    }
    return tileSize;
  }


  @Override
  public void moveOutput(String toName) throws IOException
  {
    super.moveOutput(toName);
    _outputName = toName;
    MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(_outputName,
        AccessMode.READ, getProviderProperties());
    _output = MrsPyramidDescriptor.create(dp);
  }


  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapterHadoop parser)
  {
    String usage = "CostDistance takes the following arguments " + 
        "(source point, [friction zoom level], friction raster, [maxCost])";
    Vector<ParserNode> result = new Vector<ParserNode>();
    
    if (children.size() < 2 || children.size() > 4)
    {
      throw new IllegalArgumentException(usage);
    }

    int nodeIndex = 0;
    // Add the source point
    result.add(children.get(nodeIndex));
    nodeIndex++;

    // Check for optional friction zoom level
    if (isChildInt(children.get(nodeIndex), parser))
    {
      zoomLevel = parseChildInt(children.get(nodeIndex), "friction zoom level", parser);
      nodeIndex++;
    }

    // Check that there are enough required arguments left
    if (children.size() <= nodeIndex)
    {
      throw new IllegalArgumentException(usage);
    }

    // Add the friction raster
    result.add(children.get(nodeIndex));
    nodeIndex++;

    // Check for optional max cost
    if (children.size() > nodeIndex)
    {
      maxCost = parseChildDouble(children.get(nodeIndex), "maxCost", parser);

      if (maxCost < 0)
      {
        maxCost = -1;
      }
    }    

    return result;
  }

  @Override
  public Set<String> calculateInputs() 
  {
    Set<String> inputPyramids = new HashSet<String>();
    if (_outputName != null)
    {
      inputPyramids.add(_outputName);
    }
    return inputPyramids;
  }

  /*
   * This is needed because MapAlgebraExecution calls MapOp.calculateMaximumZoomlevel, which ends
   * up calling this method recursively on all child MapOps, including potentially this one
   * 
   * (non-Javadoc)
   * @see org.mrgeo.mapreduce.MapOp#calculateMaximumZoomlevel()
   */
  @Override
  public int calculateMaximumZoomlevel()
  {
    if(zoomLevel == -1)
      throw new IllegalArgumentException("Method called too soon, zoomLevel cannot be computed at this time.");

    return zoomLevel; 
  }

  /*
   * This is needed because MapAlgebraExecutioner calls MapOp.calculateBounds, which ends
   * up calling this method recursively on all child MapOps, including potentially this one
   * 
   * (non-Javadoc)
   * @see org.mrgeo.mapreduce.MapOp#calculateBounds()
   */
  @Override
  public Bounds calculateBounds() throws IOException
  { 
    if (_output != null)
    {
      MrsImagePyramid mp = MrsImagePyramid.open(_outputName, getProviderProperties());
      return mp.getBounds();
    }
    throw new IllegalArgumentException("Method called too soon, bounds cannot be computed at this time.");
  }
  
  @Override
  public String toString()
  {
    return String.format("CostDistanceMapOp %s",
       _outputName == null ? "null" : _outputName );
  }
}
