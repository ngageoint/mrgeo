/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

package org.mrgeo.mapreduce.ingestvector;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.WritableGeometry;
import org.mrgeo.mapreduce.GeometryWritable;
import org.mrgeo.utils.geotools.GeotoolsVectorReader;
import org.mrgeo.utils.geotools.GeotoolsVectorUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.geometry.BoundingBox;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class IngestVectorRecordReader extends RecordReader<LongWritable, GeometryWritable>
{
  private static Logger log = LoggerFactory.getLogger(IngestVectorRecordReader.class);

  private GeotoolsVectorReader reader;

  private LongWritable key = new LongWritable(0);
  private GeometryWritable value = new GeometryWritable();

  private String[] attributeNames = null;
  private int geometryAttribute;

  private MathTransform transform = null;
  private CoordinateReferenceSystem epsg4326 = null;
  
  public IngestVectorRecordReader()
  {
    GeotoolsVectorUtils.initialize();
    
    try
    {
      epsg4326 = CRS.decode("EPSG:4326", true);
    }
    catch (NoSuchAuthorityCodeException e)
    {
      e.printStackTrace();
    }
    catch (FactoryException e)
    {
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws IOException
  {
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException
  {
    return key;
  }

  @Override
  public GeometryWritable getCurrentValue() throws IOException, InterruptedException
  {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException
  {
    return 0;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
  {
    final FileSplit fsplit = (FileSplit) split;

    final URI uri = fsplit.getPath().toUri();
    log.info("processing: " + uri.toString());

    reader = GeotoolsVectorUtils.open(uri);
    if (reader == null)
    {
      throw new InterruptedException("ERROR!  File not found: " + uri);
    }

//    System.out.println("Schema:");
//
//    final Class<?>[] classes = reader.getAttributeClasses();
//    final String[] names = reader.getAttributeNames();
//
//    for (int i = 0; i < names.length; i++)
//    {
//      System.out.println("  " + names[i] + ": " + classes[i].getSimpleName());
//    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    if (reader.hasNext())
    {
      final SimpleFeature feature = reader.next();

      BoundingBox b = feature.getBounds();
      CoordinateReferenceSystem crs = b.getCoordinateReferenceSystem();
      
      Geometry jtsGeometry;
      if (!crs.equals(epsg4326))
      {
        if (transform == null)
        {
          try
          {
            transform = CRS.findMathTransform(crs, epsg4326, true);
          }
          catch (FactoryException e)
          {
            e.printStackTrace();
          }
        }
        try
        {
          jtsGeometry = JTS.transform((Geometry) feature.getDefaultGeometry(), transform);
        }
        catch (MismatchedDimensionException e)
        {
          jtsGeometry = (Geometry) feature.getDefaultGeometry();
        }
        catch (TransformException e)
        {
          jtsGeometry = (Geometry) feature.getDefaultGeometry();
        }
      }
      else
      {
        jtsGeometry = (Geometry) feature.getDefaultGeometry();
      }
      
      WritableGeometry geometry = GeometryFactory.fromJTS(jtsGeometry);

      if (attributeNames == null)
      {
        List<AttributeDescriptor> schema = feature.getFeatureType().getAttributeDescriptors();
        String geomattr = feature.getDefaultGeometryProperty().getName().toString();

        attributeNames = new String[schema.size()];

        for (int i = 0; i < feature.getAttributeCount(); i++)
        {
          attributeNames[i] = schema.get(i).getName().toString();
          if (attributeNames[i] == geomattr)
          {
            geometryAttribute = i;
          }
        }
      }

      for (int i = 0; i < feature.getAttributeCount(); i++)
      {
        if (i != geometryAttribute)
        {
          geometry.setAttribute(attributeNames[i], feature.getAttribute(i).toString());
        }
      }

      key.set(key.get() + 1);
      value.set(geometry);

      return true;
    }

    return false;
  }

}
