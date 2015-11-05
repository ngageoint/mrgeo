package org.mrgeo.data.vector.geowave;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import org.mrgeo.data.CloseableKVIterator;
import org.mrgeo.data.vector.FeatureIdWritable;
import org.mrgeo.geometry.Geometry;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.WritableGeometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

import java.io.IOException;
import java.util.List;

public class GeoWaveVectorIterator implements CloseableKVIterator<FeatureIdWritable, Geometry>
{
  private CloseableIterator<?> geoWaveIter;
  private Geometry currValue;
  private FeatureIdWritable currKey = new FeatureIdWritable();

  public GeoWaveVectorIterator(CloseableIterator<?> iter)
  {
    this.geoWaveIter = iter;
  }

  @Override
  public void close() throws IOException
  {
    geoWaveIter.close();
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
  public FeatureIdWritable currentKey()
  {
    return currKey;
  }

  @Override
  public Geometry currentValue()
  {
    return currValue;
  }

  public static void setKeyFromFeature(FeatureIdWritable key, SimpleFeature feature)
  {
    // NOTE: The feature id key is not used for processing anywhere in MrGeo.
    // In fact, some of the input formats for vector data hard-code the key
    // to different values. For example CsvInputFormat hard-codes the key for
    // every record to -1. Since the key is never used, I Just hard-code it
    // here to keep things simple.
    // In the future, if needed, we should switch the FeatureIdWritable key to a
    // Text key and use the actual feature ID returned from SimpleFeature as
    // the key value. This will have a large ripple effect because input formats,
    // record readers, mappers, and reducers that used to rely on FeatureIdWritable
    // key values will have to be changed to Text, and the compiler will not be
    // able to automatically find the code that needs to change.
    key.set(-1L);
//    String id = feature.getID();
//    feature.getIdentifier();
//    if (id != null && !id.isEmpty())
//    {
//      // Id values returned from GeoWave are in the form "Name.Number"
//      // where "Name" is the name of the adapter, and "Number" is the
//      // id assignged to the feature when it was ingested. We want just
//      // the number.
//      String[] idPieces = id.split("\\.");
//      if (idPieces.length == 2)
//      {
//        key.set(Long.parseLong(idPieces[1]));
//      }
//      else
//      {
//        throw new IllegalArgumentException("Skipping record due to unexpected id value: " + id);
//      }
//    }
//    else
//    {
//      throw new IllegalArgumentException("Skipping record due to missing id");
//    }
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
