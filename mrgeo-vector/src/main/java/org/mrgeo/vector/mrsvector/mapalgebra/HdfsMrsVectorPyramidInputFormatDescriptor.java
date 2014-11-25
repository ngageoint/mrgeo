package org.mrgeo.vector.mrsvector.mapalgebra;

import org.apache.hadoop.mapreduce.Job;
import org.mrgeo.featurefilter.FeatureFilter;
import org.mrgeo.featurefilter.FeatureFilterChain;
import org.mrgeo.format.AutoFeatureInputFormat;
import org.mrgeo.format.FilteredFeatureInputFormat;
import org.mrgeo.mapalgebra.InputFormatDescriptor;
import org.mrgeo.mapreduce.formats.TileClusterInfo;
import org.mrgeo.vector.mrsvector.MrsVectorPyramid;
import org.mrgeo.utils.Bounds;
import org.mrgeo.vector.formats.HdfsMrsVectorPyramidInputFormat;

import java.io.IOException;
import java.util.Set;

public class HdfsMrsVectorPyramidInputFormatDescriptor implements InputFormatDescriptor
{
  private FeatureFilterChain _chain = new FeatureFilterChain();
  private Set<String> inputs;
  private int zoomLevel;
  // TODO: Do something useful with overallTileClusterInfo. Probably need to
  // pass it to HdfsMrsVectorPyramidInputFormat.setInputInfo().
  private TileClusterInfo overallTileClusterInfo;

  public HdfsMrsVectorPyramidInputFormatDescriptor(final int zoomLevel, final Set<String> inputs,
      final TileClusterInfo overallTileClusterInfo)
  {
    this.inputs = inputs;
    this.zoomLevel = zoomLevel;
    this.overallTileClusterInfo = overallTileClusterInfo;
  }

  public int getZoomLevel()
  {
    return zoomLevel;
  }

  @Override
  public FeatureFilter getFilter()
  {
    return _chain;
  }

  @Override
  public void populateJobParameters(Job job) throws IOException
  {
    HdfsMrsVectorPyramidInputFormat.setInputInfo(job, zoomLevel, inputs);
    FilteredFeatureInputFormat.setInputFormat(job.getConfiguration(), AutoFeatureInputFormat.class);
    FilteredFeatureInputFormat.setFeatureFilter(job.getConfiguration(), _chain);
  }
  
  public Bounds calculateBounds() throws IOException
  {
    Bounds bounds = new Bounds();
    for (String input : inputs)
    {
      MrsVectorPyramid pyramid = MrsVectorPyramid.loadPyramid(input);
      if (pyramid != null)
      {
        bounds.expand(pyramid.getBounds());
      }
    }
    return bounds;
  }
}
