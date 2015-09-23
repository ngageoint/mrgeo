package org.mrgeo.mapalgebra.old;

//import org.apache.hadoop.fs.Path;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapalgebra.BasicInputFormatDescriptor;
import org.mrgeo.mapalgebra.InlineCsvInputFormatDescriptor;
import org.mrgeo.mapalgebra.InlineCsvMapOp;
import org.mrgeo.mapalgebra.VectorMapOp;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.progress.Progress;
import org.mrgeo.rasterops.LeastCostPathCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Vector;

//import org.mrgeo.rasterops.LeastCostPathCalculator;

public class LeastCostPathMapOp extends VectorMapOp
{
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(LeastCostPathMapOp.class);

  int zoom = -1;

  @Override
  public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
  {
    String destPoints = null;
    RasterMapOpHadoop inputOp = (RasterMapOpHadoop)_inputs.get(0);
    if (inputOp.getOutputName() == null)
    {
      throw new IllegalArgumentException("Invalid raster input to LeastCostPath. The cost raster has no output name.");
    }
    String costPyramidName = inputOp.getOutputName();
    if (zoom < 0)
    {
      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(costPyramidName,
          AccessMode.READ, createConfiguration());
      if (dp != null)
      {
        MrsImagePyramidMetadata metadata = dp.getMetadataReader().read();
        zoom = metadata.getMaxZoomLevel();
      }
      else
      {
        throw new IOException("Unable to access " + costPyramidName + " to get its max zoom level.");
      }
    }

    // Extract destination points from inputs
    MapOpHadoop inlineCsvMapOp = _inputs.get(1);
    assert(inlineCsvMapOp instanceof InlineCsvMapOp);
    InlineCsvInputFormatDescriptor ifd =
            (InlineCsvInputFormatDescriptor) ((VectorMapOp)inlineCsvMapOp).getVectorOutput();
    destPoints = ifd.getValues();
    
    LeastCostPathCalculator.run(destPoints, costPyramidName, zoom, _outputName,
                                getProviderProperties());
    
    BasicInputFormatDescriptor result = new BasicInputFormatDescriptor(_outputName);
        
    _output = result;
  }
  
  @Override
  public void moveOutput(String toName) throws IOException
  {
    super.moveOutput(toName);
    _outputName = toName;
    _output = new BasicInputFormatDescriptor(_outputName);
  }

  @Override
  public void addInput(MapOpHadoop n) throws IllegalArgumentException
  {
    if (_inputs.size() == 0)
    {
      if (!(n instanceof RasterMapOpHadoop))
      {
        throw new IllegalArgumentException("The cost argument must be a raster.");
      }
    }
    else if (_inputs.size() == 1 && !(n instanceof VectorMapOp))
    {
      throw new IllegalArgumentException("The destination points argument must be a vector input");
    }

    _inputs.add(n);
  }
   
  @Override
  public Vector<ParserNode> processChildren(final Vector<ParserNode> children, final ParserAdapterHadoop parser)
  {
    Vector<ParserNode> result = new Vector<ParserNode>();
    
    if (children.size() != 2 && children.size() != 3)
    {
      throw new IllegalArgumentException(
          "LeastCostPath takes the following arguments ([cost zoom level], cost raster, destination points");
    }
    
    int nodeIndex = 0;
    if (children.size() == 3)
    {
      zoom = parseChildInt(children.get(nodeIndex), "cost zoom", parser);
      nodeIndex++;
    }
    result.add(children.get(nodeIndex));
    nodeIndex++;
    result.add(children.get(nodeIndex));
    nodeIndex++;

    return result;
  }
  
  @Override
  public String toString()
  {
    return String.format("LeastCostMapOp %s",
       _outputName == null ? "null" : _outputName );
  }
}
