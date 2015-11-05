package org.mrgeo.data.vector.geowave;

import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputFormat;
import org.apache.hadoop.mapreduce.*;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.geometry.Geometry;

import java.io.IOException;
import java.util.List;

public class GeoWaveVectorInputFormat extends InputFormat<FeatureIdWritable, Geometry>
{
  private GeoWaveInputFormat delegate = new GeoWaveInputFormat();

  public GeoWaveVectorInputFormat()
  {
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    try
    {
      return delegate.getSplits(context);
    }
    catch(OutOfMemoryError e)
    {
      // This can happen for example if the date range used in the temporal query
      // spans too much time. I'm not sure what else might trigger this.
      throw new IOException("Unable to query GeoWave data due to memory constraints." +
                            " If you queried by a time range, the range may be too large.", e);
    }
  }

  @Override
  public RecordReader<FeatureIdWritable, Geometry> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException
  {
    RecordReader<FeatureIdWritable, Geometry> result = new GeoWaveVectorRecordReader();
    result.initialize(split, context);
    return result;
  }
}
