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

package org.mrgeo.services.utils;

import org.gdal.osr.CoordinateTransformation;
import org.gdal.osr.SpatialReference;
import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.utils.GDALUtils;
import org.mrgeo.utils.tms.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * General utilities for dealing with WMS requests
 */
public class RequestUtils
{
private static final Logger log = LoggerFactory.getLogger(RequestUtils.class);

static
{
  GDALUtils.register();
}

public static String buildBaseURI(UriInfo uriInfo, HttpHeaders headers)
{
  URI base = uriInfo.getBaseUriBuilder().path(uriInfo.getPath()).build();

  List<String> proxies = headers.getRequestHeader("X-Forwarded-For");
  if (proxies == null || proxies.size() < 1)
  {
    return base.toASCIIString();
  }
  else
  {
    try
    {
      URI proxy = new URI("http://" + proxies.get(0));  // the 1st proxy is the original one...

      return new URI(base.getScheme(), base.getRawUserInfo(), proxy.getHost(), proxy.getPort(),
          base.getRawPath(), base.getRawQuery(), base.getRawFragment()).toASCIIString();
    }
    catch (URISyntaxException e)
    {
      return base.toASCIIString();
    }
  }
}

public static String buildURI(String baseUri, MultivaluedMap<String, String> params)
{
  UriBuilder builder = UriBuilder.fromUri(baseUri);

  for (MultivaluedMap.Entry<String, List<String>> param : params.entrySet())
  {
    for (String v: param.getValue())
    {
      builder.queryParam(param.getKey(), v);
    }
  }

  return builder.build().toASCIIString();
}

public static MultivaluedMap<String, String> replaceParam(String name, String value, MultivaluedMap<String, String> params)
{
  return replaceParam(name, Collections.singletonList(value), params);

}

public static MultivaluedMap<String, String> replaceParam(String name, List<String> values,
    MultivaluedMap<String, String> params)
{

  MultivaluedStringMap sm = new MultivaluedStringMap(params);

  for (Map.Entry<String, List<String>> es : params.entrySet())
  {
    if (es.getKey().equalsIgnoreCase(name))
    {
      sm.remove(es.getKey());
      break;
    }
  }

  sm.putIfAbsent(name, values);

  return sm;
}


/*
 * Returns a list of all MrsPyramid version 2 data in the home data directory
 */
public static MrsImageDataProvider[] getPyramidFilesList(
    ProviderProperties providerProperties) throws IOException
{
  String[] images = DataProviderFactory.listImages(providerProperties);

  Arrays.sort(images);

  MrsImageDataProvider[] providers = new MrsImageDataProvider[images.length];

  for (int i = 0; i < images.length; i++)
  {
    providers[i] = DataProviderFactory.getMrsImageDataProvider(images[i],
        DataProviderFactory.AccessMode.READ,
        providerProperties);
  }

  return providers;
}


/**
 * Parses a geographic bounds from a request parameter value
 *
 * @param param request parameter value
 * @return geographic bounds
 */
static Bounds boundsFromParam(String param)
{
  if (param == null)
  {
    throw new IllegalArgumentException("Bounding box must be specified.");
  }
  log.debug("incoming bounds request: " + param);
  String[] bBoxValues = param.split(",");
  if (bBoxValues.length != 4)
  {
    throw new IllegalArgumentException("Bounding box must have four comma delimited arguments.");
  }
  return new Bounds(Double.valueOf(bBoxValues[0]), Double.valueOf(bBoxValues[1]),
      Double.valueOf(bBoxValues[2]), Double.valueOf(bBoxValues[3]));
}

/**
 * Reprojects a bounds to Geographic
 *
 * @param bounds the projected input bounds
 * @param srs   the spatial reference of the projected bounds crs. The format is described
 *              in http://gdal.org/java/org/gdal/osr/SpatialReference.html#SetFromUserInput(java.lang.String).
 *              Examples include "EPSG:4326" and "CRS:84".
 * @return geographic bounds
 */
public static Bounds reprojectBounds(Bounds bounds, String srs)
{
  if (srs != null && !(srs.equalsIgnoreCase("EPSG:4326")))
  {
    SpatialReference src = new SpatialReference(GDALUtils.EPSG4326());
    SpatialReference dst = new SpatialReference();
    dst.SetFromUserInput(srs);

    CoordinateTransformation tx = new CoordinateTransformation(src, dst);

    double[] c1;
    double[] c2;
    double[] c3;
    double[] c4;

    c1 = tx.TransformPoint(bounds.w, bounds.s);
    c2 = tx.TransformPoint(bounds.w, bounds.n);
    c3 = tx.TransformPoint(bounds.e, bounds.s);
    c4 = tx.TransformPoint(bounds.e, bounds.n);

    return new Bounds(Math.min(Math.min(c1[0], c2[0]), Math.min(c3[0], c4[0])),
        Math.min(Math.min(c1[1], c2[1]), Math.min(c3[1], c4[1])),
        Math.max(Math.max(c1[0], c2[0]), Math.max(c3[0], c4[0])),
        Math.max(Math.max(c1[1], c2[1]), Math.max(c3[1], c4[1])));
  }
  else
  {
    return bounds.clone();
  }
}

/**
 * Reprojects a bounds to WGS-84
 *
 * @param bounds the projected input bounds
 * @param srs   the spatial reference of the projected bounds crs. The format is
 *              described in http://gdal.org/java/org/gdal/osr/SpatialReference.html#SetFromUserInput(java.lang.String)
 *              Examples include "EPSG:4326" and "CRS:84".
 * @return geographic bounds
 */
public static Bounds reprojectBoundsToWGS84(Bounds bounds, String srs)
{
  if (srs != null)
  {
    SpatialReference src = new SpatialReference();
    src.SetFromUserInput(srs);

    SpatialReference dst = new SpatialReference(GDALUtils.EPSG4326());
    CoordinateTransformation tx = new CoordinateTransformation(src, dst);

    double[] c1;
    double[] c2;
    double[] c3;
    double[] c4;

    c1 = tx.TransformPoint(bounds.w, bounds.s);
    c2 = tx.TransformPoint(bounds.w, bounds.n);
    c3 = tx.TransformPoint(bounds.e, bounds.s);
    c4 = tx.TransformPoint(bounds.e, bounds.n);

    return new Bounds(Math.min(Math.min(c1[0], c2[0]), Math.min(c3[0], c4[0])),
        Math.min(Math.min(c1[1], c2[1]), Math.min(c3[1], c4[1])),
        Math.max(Math.max(c1[0], c2[0]), Math.max(c3[0], c4[0])),
        Math.max(Math.max(c1[1], c2[1]), Math.max(c3[1], c4[1])));
  }
  return bounds.clone();
}
}