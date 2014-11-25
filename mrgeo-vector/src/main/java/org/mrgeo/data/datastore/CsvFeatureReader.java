package org.mrgeo.data.datastore;

import com.vividsolutions.jts.geom.Geometry;
import org.geotools.data.FeatureReader;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.mrgeo.data.csv.CsvGeometryInputStream;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.NoSuchElementException;

public class CsvFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature>
{
  CsvGeometryInputStream stream;
  SimpleFeatureType featureType = null;
  String[] saveCols = null;

  public CsvFeatureReader(CsvGeometryInputStream stream)
  {
    this.stream = stream;
    
  }

  @Override
  public SimpleFeatureType getFeatureType()
  {

    return null;
  }

  @Override
  public SimpleFeature next() throws IOException, IllegalArgumentException, NoSuchElementException
  {
    String[] columns;
    if (saveCols != null)
    {  
      columns = saveCols;
      saveCols = null;
    }
    else
    {
      columns = stream.next();
    }

    if (featureType == null)
    {
      featureType = buildFeatureType(stream.getHeader(), columns);
    }

    return SimpleFeatureBuilder.build(featureType, attributes(columns), "" + stream.getRecordNumber());
  }

  private static Object[] attributes(String[] columns)
  {
    Object[] attributes = new Object[columns.length];
    for (int i = 0; i < columns.length; i++)
    {
      attributes[i] = CsvGeometryInputStream.parseType(columns[i]);
    }
    return attributes;
  }

  @Override
  public boolean hasNext() throws IOException
  {
    return stream.hasNext();
  }

  @Override
  public void close() throws IOException
  {
    stream.close();
  }
  
  public SimpleFeatureType featureType() throws IOException
  {
    if (featureType == null)
    {
      if (hasNext())
      {
        saveCols = stream.next();
        featureType = buildFeatureType(stream.getHeader(), saveCols);
      }
    }
    
    return featureType;
  }

  private static SimpleFeatureType buildFeatureType(String[] header, String[] cols)
  {
    // create the builder
    SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();

    // set global state
    builder.setName( "CSVFeatures" );
    builder.setNamespaceURI("");
    builder.setSRS( "EPSG:4326" );

    String geometry = null;
    
    for (int i = 0; i < cols.length; i++)
    {
      Object o = CsvGeometryInputStream.parseType(cols[i]);

      String name;
      if (header != null && i < header.length)
      {
        name = header[i];
      }
      else
      {
        name = "field-" + i;
      }
      
      if (o instanceof Geometry)
      {
        geometry = name;
      }

      builder.add(name, o.getClass());
    }
    
    // add the geometry
    builder.setDefaultGeometry(geometry);

    //build the type
    return builder.buildFeatureType();

  }

}
