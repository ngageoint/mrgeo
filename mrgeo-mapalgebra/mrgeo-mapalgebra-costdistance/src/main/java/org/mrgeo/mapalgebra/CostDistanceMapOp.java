package org.mrgeo.mapalgebra;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.opimage.MrsPyramidDescriptor;
import org.mrgeo.progress.Progress;
import org.mrgeo.spark.CostDistanceDriver;
import org.mrgeo.utils.Bounds;

public class CostDistanceMapOp extends RasterMapOp
  implements InputsCalculator, BoundsCalculator, TileSizeCalculator, MaximumZoomLevelCalculator
{
  double maxCost = -1;
  int zoomLevel = -1;
  int tileSize = -1;
  Bounds bounds;

  public static String[] register()
  {
    return new String[] { "CostDistance" };
  }

  @Override
  public void addInput(MapOp n) throws IllegalArgumentException
  {
    if (_inputs.size() == 1)
    {
      // The friction surface is now being added - make sure it is a raster
      if (!(n instanceof RasterMapOp))
      {
        throw new IllegalArgumentException("The second argument to CostDistance is the friction surface which must be an ingested image");
      }
      if (((RasterMapOp)n).getOutputName() == null)
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
    
    MapOp inlineCsvMapOp = _inputs.get(0);
    
    // Extract source point from vector input
    assert(inlineCsvMapOp instanceof InlineCsvMapOp);
     InlineCsvInputFormatDescriptor ifd =  
             (InlineCsvInputFormatDescriptor) ((InlineCsvMapOp)inlineCsvMapOp).getVectorOutput();
     String sourcePoints = ifd.getValues();

    // Get friction input
    RasterMapOp friction = (RasterMapOp) _inputs.get(1);

    if (zoomLevel < 0)
    {
      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(friction.getOutputName(), AccessMode.READ,
          getProviderProperties());
      MrsImagePyramidMetadata metadata = dp.getMetadataReader().read();
      zoomLevel = metadata.getMaxZoomLevel();
    }

    try {
      CostDistanceDriver.costDistance(friction.getOutputName(), _outputName, zoomLevel, bounds,
                                      sourcePoints, maxCost, createConfiguration());
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
      RasterMapOp friction = (RasterMapOp) _inputs.get(1);
      
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
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapter parser)
  {
    String usage = "CostDistance takes the following arguments " + 
        "(source point, [friction zoom level], friction raster, [maxCost], [minX, minY, maxX, maxY])";
    Vector<ParserNode> result = new Vector<ParserNode>();
    
    if (children.size() < 2 || children.size() > 8)
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

    // Check for optional max cost. Both max cost and bounds are optional, so there
    // must be either 1 argument left or 5 arguments left in order for max cost to
    // be included.
    if ((children.size() == (nodeIndex + 1)) || (children.size() == (nodeIndex + 5)))
    {
      maxCost = parseChildDouble(children.get(nodeIndex), "maxCost", parser);
      nodeIndex++;

      if (maxCost < 0)
      {
        maxCost = -1;
      }
    }

    // Check for optional bounds
    if (children.size() > nodeIndex)
    {
      if (children.size() == nodeIndex + 4)
      {
        final double[] b = new double[4];
        for (int i = 0; i < 4; i++)
        {
          b[i] = parseChildDouble(children.get(nodeIndex), "bounds", parser);
          nodeIndex++;
        }
        bounds = new Bounds(b[0], b[1], b[2], b[3]);
      }
      else
      {
        throw new IllegalArgumentException("The bounds argument must include minX, minY, maxX and maxY");
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
