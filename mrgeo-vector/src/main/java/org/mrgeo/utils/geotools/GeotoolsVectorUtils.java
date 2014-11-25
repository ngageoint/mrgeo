package org.mrgeo.utils.geotools;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.geotools.data.*;
import org.geotools.referencing.CRS;
import org.mrgeo.geometry.GeometryFactory;
import org.mrgeo.geometry.WritableGeometry;
import org.mrgeo.image.geotools.GeotoolsRasterUtils;
import org.mrgeo.vector.mrsvector.WritableVectorTile;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.Bounds;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.geometry.BoundingBox;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.*;

public class GeotoolsVectorUtils
{
  private static final Logger log = LoggerFactory.getLogger(GeotoolsVectorUtils.class);

  private static List<DataStoreFactorySpi> formats = null;

  static
  {
    // this will force geotools initialization...
    GeotoolsRasterUtils.addMissingEPSGCodes();

    // this enables hdfs access through a URL
//    URL.setURLStreamHandlerFactory(new HadoopURLStreamHandlerFactory());
  }

  private GeotoolsVectorUtils()
  {
  }

  public static void initialize()
  {
    // no op to run static initializer block...
  }

  public static boolean fastAccepts(final Map<String, Serializable> params)
  {
    if (formats == null)
    {
      loadFormats();
    }

    for (final DataStoreFactorySpi format : formats)
    {
      if (format.canProcess(params))
      {
        return true;
      }
    }
    return false;
  }

  public static DataStoreFactorySpi fastFormatFinder(final Map<String, Serializable> params)
  {
    if (formats == null)
    {
      loadFormats();
    }

    for (final DataStoreFactorySpi format : formats)
    {
      if (format.canProcess(params))
      {
        return format;
      }
    }
    return null;
  }


  public static GeotoolsVectorReader open(final URI uri) throws IOException, MalformedURLException
  {
    return GeotoolsVectorReader.open(uri);
  }

  @SuppressWarnings("unused")
  public static DataAccessFactory.Param[] params(final String type)
  {
    throw new NotImplementedException("params() not yet implemeted!");

    // for (final DataStoreFactorySpi format : formats)
    // {
    // final DataAccessFactory.Param[] params = format.getParametersInfo();
    // // if (params["url"].getName() == name)
    // // {
    // // return param;
    // // }
    // }
    //
    // return null;
  }

  public static Bounds calculateBounds(GeotoolsVectorReader reader)
  {
    Bounds bounds = null;

    boolean first = true;
    boolean transform = false;
    CoordinateReferenceSystem epsg4326;
    try
    {
      epsg4326 = CRS.decode("EPSG:4326", true);

      while (reader.hasNext())
      {
        SimpleFeature feature = reader.next();

        BoundingBox bbox = feature.getBounds();
        if (first)
        {
          CoordinateReferenceSystem crs = bbox.getCoordinateReferenceSystem();

          transform = !crs.equals(epsg4326);
          first = true;
        }
        if (transform)
        {
          try
          {
            bbox =  bbox.toBounds(epsg4326);
          }
          catch (TransformException e)
          {
            e.printStackTrace();
          }
        }

        Bounds b = new Bounds(bbox.getMinX(), bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY());
        if (bounds == null)
        {
          bounds = b;
        }
        else
        {
          bounds.expand(b);
        }
      }
    }
    catch (NoSuchAuthorityCodeException e)
    {
      e.printStackTrace();
    }
    catch (FactoryException e)
    {
      e.printStackTrace();
    }

    return bounds;
  }

  public static Bounds calculateBounds(String[] inputs, Configuration conf) throws IOException
  {
    log.info("Calculating bounds");

    GeotoolsVectorReader reader = null;

    Bounds bounds = null;

    FileSystem fs = HadoopFileUtils.getFileSystem(conf);

    for (String input: inputs)
    {
      try
      {
        Path p = new Path(input);
        URI uri = p.makeQualified(fs).toUri();
        reader = GeotoolsVectorUtils.open(uri);

        Bounds b = calculateBounds(reader);

        if (bounds == null)
        {
          bounds = b;
        }
        else
        {
          bounds.expand(b);
        }
      }
//      catch (IOException e)
//      {
//        e.printStackTrace();
//      }
      finally
      {
        if (reader != null)
        {
          reader.close();
          reader = null;
        }
      }
    }

    log.info("Bounds {}", bounds);

    return bounds;
  }


  private static void loadFormats()
  {
    DataStoreFinder.scanForPlugins();

    // order the formats so mrgeo ones are first
    formats = new LinkedList<DataStoreFactorySpi>();

    final HashSet<DataStoreFactorySpi> unorderedformats = new HashSet<DataStoreFactorySpi>();

    final Iterator<DataStoreFactorySpi> spis = DataStoreFinder.getAllDataStores();

    while (spis.hasNext())
    {
      unorderedformats.add(spis.next());
    }

    final HashSet<DataStoreFactorySpi> tmpformats = new HashSet<DataStoreFactorySpi>();
    for (final DataStoreFactorySpi format : unorderedformats)
    {
      if (format.getClass().getCanonicalName().startsWith("org.mrgeo"))
      {
        formats.add(format);
      }
      else
      {
        tmpformats.add(format);
      }
    }
    formats.addAll(tmpformats);
  }

}
