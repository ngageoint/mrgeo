/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.data.vector.geowave;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.geometry.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GeoWaveVectorRecordReader extends RecordReader<FeatureIdWritable, Geometry>
{
public static final String CQL_FILTER = GeoWaveVectorRecordReader.class.getName() + ".cqlFilter";
static Logger log = LoggerFactory.getLogger(GeoWaveVectorRecordReader.class);
private RecordReader delegateReader;
private FeatureIdWritable currKey = new FeatureIdWritable();
private Geometry currValue;
private Filter cqlFilter;
private String strCqlFilter;
private long featureCount = 0L;

public GeoWaveVectorRecordReader(RecordReader delegateReader)
{
  this.delegateReader = delegateReader;
}

@Override
public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
    InterruptedException
{
  delegateReader.initialize(split, context);
  // No need to initialize the delegateReader since it was already done in the
  // GeoWaveInputFormat.createRecordReader method
  strCqlFilter = context.getConfiguration().get(CQL_FILTER);
  if (strCqlFilter != null && !strCqlFilter.isEmpty())
  {
    try
    {
      log.info("Creating the CQL filter");
      cqlFilter = ECQL.toFilter(strCqlFilter);
      log.info("Done creating the CQL filter");
    }
    catch (CQLException e)
    {
      throw new IOException("Unable to instantiate CQL filter for: " + strCqlFilter, e);
    }
  }
}

@Override
public boolean nextKeyValue() throws IOException, InterruptedException
{
  // Get records from the delegate record reader. If there is a CQL filter
  // being applied, loop until the returned value satisfies that filter or
  // there are no more records.
//    log.info("Calling GeoWave delegate reader nextKeyValue()");
  boolean result = delegateReader.nextKeyValue();
//    log.info("Done calling GeoWave delegate reader nextKeyValue()");
  while (result)
  {
    Object value = delegateReader.getCurrentValue();
    boolean matchesFilter = (cqlFilter != null) ? cqlFilter.evaluate(value) : true;
    if (matchesFilter)
    {
      if (value instanceof SimpleFeature)
      {
        SimpleFeature feature = (SimpleFeature) value;
        featureCount++;
        GeoWaveVectorIterator.setKeyFromFeature(currKey, feature);
        currValue = GeoWaveVectorIterator.convertToGeometry(feature);
      }
      else
      {
        throw new IOException("Expected value of type SimpleFeature, but got " + value.getClass().getName());
      }
      return true;
    }
//      log.info("Calling GeoWave delegate reader nextKeyValue() in while loop");
    result = delegateReader.nextKeyValue();
//      log.info("Done calling GeoWave delegate reader nextKeyValue() in while loop");
  }
  currKey = null;
  currValue = null;
  return result;
}

@Override
public FeatureIdWritable getCurrentKey() throws IOException, InterruptedException
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
  log.info("GeoWave feature count from reader = " + featureCount);
}
}
