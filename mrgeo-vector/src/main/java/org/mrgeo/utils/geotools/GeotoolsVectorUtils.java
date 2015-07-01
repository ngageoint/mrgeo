/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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

package org.mrgeo.utils.geotools;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.geotools.data.DataAccessFactory;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.DataStoreFinder;
import org.geotools.factory.Hints;
import org.geotools.metadata.iso.citation.Citations;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.geotools.referencing.factory.PropertyAuthorityFactory;
import org.geotools.referencing.factory.ReferencingFactoryContainer;
import org.geotools.util.logging.Logging;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LoggingUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.BoundingBox;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import javax.media.jai.JAI;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.*;
import java.util.logging.LogManager;

public class GeotoolsVectorUtils
{
private static final Logger log = LoggerFactory.getLogger(GeotoolsVectorUtils.class);

static
{
  try
  {
    // lower the log level of geotools (it spits out _way_ too much logging)
    LoggingUtils.setLogLevel("org.geotools", LoggingUtils.ERROR);

    // connect geotools to slf4j
    Logging.GEOTOOLS.setLoggerFactory("org.geotools.util.logging.Log4JLoggerFactory");


    // connect imageio-ext (Java logging (JUL)) to slf4j and set its level
    LogManager.getLogManager().reset();
    SLF4JBridgeHandler.install();

    String defaultLevel = LoggingUtils.getDefaultLogLevel();

    if (LoggingUtils.finer(defaultLevel, LoggingUtils.WARN))
    {
      LoggingUtils.setLogLevel("it.geosolutions", defaultLevel);
    }
    else
    {
      LoggingUtils.setLogLevel("it.geosolutions", LoggingUtils.ERROR);
    }
  }

  catch (final Exception e)
  {
    e.printStackTrace();
  }

  // force geotools to use y, x (lat, lon) ordering. This is the default, but we'll set
  // this property here just in case
  System.setProperty("org.geotools.referencing.forceXY", "false");

  addMissingEPSGCodesInternal();

  final ClassLoader cl = Thread.currentThread().getContextClassLoader();
  try
  {
    cl.loadClass("com.sun.medialib.mlib.Image");
  }
  catch (final ClassNotFoundException e)
  {
    // native acceleration is not loaded, disable it...
    System.setProperty("com.sun.media.jai.disableMediaLib", "true");
  }

  JAI.disableDefaultTileCache();
}

private static boolean epsgLoaded = false;

private static synchronized void addMissingEPSGCodesInternal()
{
  if (epsgLoaded)
  {
    return;
  }

  epsgLoaded = true;

  log.info("Loading missing epsg codes");

  // there are non-standard EPSG codes (like Google's 900913) missing from the
  // main database.
  // this code loads them from an epsg properties file. To add additional
  // codes, just add them to
  // the property file as wkt

  // increase the tile cache size to speed things up, 256MB
  final long memCapacity = 268435456;
  if (JAI.getDefaultInstance().getTileCache().getMemoryCapacity() < memCapacity)
  {
    JAI.getDefaultInstance().getTileCache().setMemoryCapacity(memCapacity);
  }

  try
  {
    final URL epsg = GeotoolsVectorUtils.class.getResource("epsg.properties");

    if (epsg != null)
    {
      final Hints hints = new Hints(Hints.CRS_AUTHORITY_FACTORY, PropertyAuthorityFactory.class,
          Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.FALSE);
      final ReferencingFactoryContainer referencingFactoryContainer = ReferencingFactoryContainer
          .instance(hints);

      final PropertyAuthorityFactory factory = new PropertyAuthorityFactory(
          referencingFactoryContainer, Citations.fromName("EPSG"), epsg);

      ReferencingFactoryFinder.addAuthorityFactory(factory);
      ReferencingFactoryFinder.scanForPlugins(); // hook everything up
    }
    else
    {
      log.error("epsg code resource (epsg.properties) missing from " +
          GeotoolsVectorUtils.class.getPackage().getName() + " package");
    }
  }
  // Set<String> codes = CRS.getSupportedCodes("EPSG");
  // System.out.println(codes.toString());
  // CoordinateReferenceSystem crs = CRS.decode("EPSG:900913");
  // }
  // catch (FactoryException e)
  // {
  // e.printStackTrace();
  // }
  catch (final IOException e)
  {
    e.printStackTrace();
  }
}

public static void addMissingEPSGCodes()
{
  // noop. The static block above will force a call to addMissingEPSGCodesInternal()...
}

private static List<DataStoreFactorySpi> formats = null;


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
