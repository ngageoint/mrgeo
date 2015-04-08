package org.mrgeo.data.vector.geowave;

import java.io.IOException;

import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveRecordReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.mrgeo.data.vector.VectorInputSplit;
import org.mrgeo.geometry.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

public class GeoWaveVectorRecordReader extends RecordReader<LongWritable, Geometry>
{
  public static final String CQL_FILTER = GeoWaveVectorRecordReader.class.getName() + ".cqlFilter";

  private GeoWaveRecordReader<Object> delegateReader;
  private LongWritable currKey = new LongWritable();
  private Geometry currValue;
  private Filter cqlFilter;
  private String strCqlFilter;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException
  {
    if (!(split instanceof VectorInputSplit))
    {
      throw new IOException("Expected split to be of type VectorInputSplit, but got: " + split.getClass().getName());
    }
    strCqlFilter = context.getConfiguration().get(CQL_FILTER);
    if (strCqlFilter != null && !strCqlFilter.isEmpty())
    {
      try
      {
        cqlFilter = ECQL.toFilter(strCqlFilter);
      }
      catch (CQLException e)
      {
        throw new IOException("Unable to instantiate CQL filter for: " + strCqlFilter, e);
      }
    }
    delegateReader = new GeoWaveRecordReader<Object>();
    // Pass the native split wrapped by VectorInputSplit back into the native reader.
    delegateReader.initialize(((VectorInputSplit)split).getWrappedInputSplit(), context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    // Get records from the delegate record reader. If there is a CQL filter
    // being applied, loop until the returned value satisfies that filter or
    // there are no more records.
    boolean result = delegateReader.nextKeyValue();
    while (result)
    {
      Object value = delegateReader.getCurrentValue();
      boolean matchesFilter = (cqlFilter != null) ? cqlFilter.evaluate(value) : true;
      if (matchesFilter)
      {
        if (value instanceof SimpleFeature)
        {
          SimpleFeature feature = (SimpleFeature) value;
          GeoWaveVectorIterator.setKeyFromFeature(currKey, feature);
          currValue = GeoWaveVectorIterator.convertToGeometry(feature);
        }
        else
        {
          throw new IOException("Expected value of type SimpleFeature, but got " + value.getClass().getName());
        }
        return true;
      }
      result = delegateReader.nextKeyValue();
    }
    if (!result)
    {
      currKey = null;
      currValue = null;
    }
    return result;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException
  {
    return currKey;
  }

  @Override
  public Geometry getCurrentValue() throws IOException, InterruptedException
  {
    return currValue;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException
  {
    return delegateReader.getProgress();
  }

  @Override
  public void close() throws IOException
  {
    delegateReader.close();
  }
}
