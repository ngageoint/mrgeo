package org.mrgeo.data.vector.geowave;

import java.util.List;

import mil.nga.giat.geowave.store.CloseableIterator;

import org.apache.hadoop.io.LongWritable;
import org.mrgeo.data.KVIterator;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.WritableGeometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

public class GeoWaveVectorIterator implements KVIterator<LongWritable, Geometry>
{
  private CloseableIterator<?> geoWaveIter;
  private Geometry currValue;
  private LongWritable currKey = new LongWritable();

  public GeoWaveVectorIterator(CloseableIterator<?> iter)
  {
    this.geoWaveIter = iter;
  }

  @Override
  public boolean hasNext()
  {
    return geoWaveIter.hasNext();
  }

  @Override
  public Geometry next()
  {
    Object value = geoWaveIter.next();
    if (value != null)
    {
      if (value instanceof SimpleFeature)
      {
        final SimpleFeature sf = (SimpleFeature) value;
        setKeyFromFeature(currKey, sf);
        currValue = convertToGeometry(sf);
        return currValue;
      }
      throw new IllegalArgumentException("Invalid value returned from GeoWave iterator. Expected SimpleFeature but got " + value.getClass().getName());
    }
    else
    {
      currValue = null;
      currKey = null;
    }
    return currValue;
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException("iterator is read-only");
  }

  @Override
  public LongWritable currentKey()
  {
    return currKey;
  }

  @Override
  public Geometry currentValue()
  {
    return currValue;
  }

  public static void setKeyFromFeature(LongWritable key, SimpleFeature feature)
  {
    String id = feature.getID();
    if (id != null && !id.isEmpty())
    {
      // Id values returned from GeoWave are in the form "Name.Number"
      // where "Name" is the name of the adapter, and "Number" is the
      // id assignged to the feature when it was ingested. We want just
      // the number.
      String[] idPieces = id.split("\\.");
      if (idPieces.length == 2)
      {
        key.set(Long.parseLong(idPieces[1]));
      }
      else
      {
        throw new IllegalArgumentException("Skipping record due to unexpected id value: " + id);
      }
    }
    else
    {
      throw new IllegalArgumentException("Skipping record due to missing id");
    }
  }

  public static WritableGeometry convertToGeometry(SimpleFeature feature)
  {
    // Translate the SimpleFeature geometry to JTS geometry
    com.vividsolutions.jts.geom.Geometry jtsGeometry = (com.vividsolutions.jts.geom.Geometry)feature.getDefaultGeometry();
    // Then generate a MrGeo WritableGeometry from the JTS geometry
    WritableGeometry geom = GeometryFactory.fromJTS(jtsGeometry);
    // Now copy over the attributes to the MrGeo WritableGeometry
    List<AttributeDescriptor> descriptors = feature.getFeatureType().getAttributeDescriptors();
    for (AttributeDescriptor desc : descriptors)
    {
      String localName = desc.getLocalName();
      Object attrValue = feature.getAttribute(localName);
      if (attrValue != null)
      {
        String strValue = attrValue.toString();
        geom.setAttribute(localName, strValue);
      }
    }
    return geom;
  }
}
