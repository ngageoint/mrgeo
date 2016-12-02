/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
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
 *
 */

package org.mrgeo.services.mrspyramid.rendering;

import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.services.ServletUtils;
import org.mrgeo.utils.GDALJavaUtils;
import org.mrgeo.utils.tms.Bounds;
import org.mrgeo.utils.tms.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class GeotiffImageResponseWriter extends TiffImageResponseWriter
{
private static final Logger log = LoggerFactory.getLogger(GeotiffImageResponseWriter.class);

public GeotiffImageResponseWriter()
{
}

@Override
public String[] getMimeTypes()
{
  return new String[]{"image/geotiff", "image/geotif"};
}

@Override
public String getResponseMimeType()
{
  return "image/geotiff";
}

@Override
public String[] getWmsFormats()
{
  return new String[]{"geotiff", "geotif"};
}

@Override
public Response.ResponseBuilder write(final MrGeoRaster raster)
{
  try
  {
    final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

    writeStream(raster, Bounds.WORLD, Double.NaN, byteStream);

    Response.ResponseBuilder response = Response.ok();
    response.entity(byteStream.toByteArray());
    response.encoding(getResponseMimeType());
    response.header("Content-type", getResponseMimeType());
    response.header("Content-Disposition", "attachment; filename=image.tif");

    return response;
  }
  catch (IOException e)
  {
    log.error("Exception thrown", e);
    if (e.getMessage() != null)
    {
      return Response.serverError().entity(e.getMessage());
    }
    return Response.serverError().entity("Internal Error");
  }
}

@Override
public void write(final MrGeoRaster raster, final HttpServletResponse response)
    throws ServletException
{
  response.setContentType(getResponseMimeType());
  final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
  try
  {
    writeStream(raster, Bounds.WORLD, Double.NaN, byteStream);
    ServletUtils.writeImageToResponse(response, byteStream.toByteArray());
  }
  catch (final IOException e)
  {
    throw new ServletException("Error writing raster", e);
  }
}

@Override
public Response.ResponseBuilder write(final MrGeoRaster raster, final int tileColumn, final int tileRow,
    final double scale, final MrsPyramid pyramid)
{
  try
  {
    final int tilesize = pyramid.getMetadata().getTilesize();

    final int zoom = TMSUtils.zoomForPixelSize(scale, tilesize);
    final Bounds bounds = TMSUtils.tileBounds(tileColumn, tileRow, zoom, tilesize);
    final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

    writeStream(raster, bounds, pyramid.getMetadata().getDefaultValue(0), byteStream);

    Response.ResponseBuilder response = Response.ok().entity(byteStream.toByteArray())
        .encoding(getResponseMimeType())
        .header("Content-type", getResponseMimeType())
        .header("Content-Disposition", "attachment; filename=" + pyramid.getName() + ".tif");

    return response;
  }
  catch (final Exception e)
  {
    log.error("Exception thrown", e);
    if (e.getMessage() != null)
    {
      return Response.serverError().entity(e.getMessage());
    }
    return Response.serverError().entity("Internal Error");

  }

}

@Override
public void write(final MrGeoRaster raster, final int tileColumn, final int tileRow,
    final double scale, final MrsPyramid pyramid, final HttpServletResponse response)
    throws ServletException
{
  try
  {
    final int tilesize = pyramid.getMetadata().getTilesize();

    final int zoom = TMSUtils.zoomForPixelSize(scale, tilesize);
    final Bounds bounds = TMSUtils.tileBounds(tileColumn, tileRow, zoom, tilesize);

    response.setContentType(getResponseMimeType());
    final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    writeStream(raster, bounds, pyramid.getMetadata().getDefaultValue(0), byteStream);
    ServletUtils.writeImageToResponse(response, byteStream.toByteArray());
  }
  catch (final IOException e)
  {
    throw new ServletException("Error writing raster", e);
  }

}

@Override
public Response.ResponseBuilder write(final MrGeoRaster raster, final String imageName, final Bounds bounds)
{
  try
  {
    final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

    MrsImageDataProvider dp =
        DataProviderFactory.getMrsImageDataProvider(imageName, DataProviderFactory.AccessMode.READ,
            (ProviderProperties) null);
    MrsPyramidMetadata metadata = dp.getMetadataReader().read();

    writeStream(raster, bounds, metadata.getDefaultValue(0), byteStream);

    Response.ResponseBuilder response = Response.ok().entity(byteStream.toByteArray())
        .encoding(getResponseMimeType())
        .header("Content-type", getResponseMimeType())
        .header("Content-Disposition", "attachment; filename=" + imageName + ".tif");

    return response;

  }
  catch (IOException e)
  {
    log.error("Exception thrown", e);
    if (e.getMessage() != null)
    {
      return Response.serverError().entity(e.getMessage());
    }
    return Response.serverError().entity("Internal Error");
  }
}

@Override
public void write(final MrGeoRaster raster, final String imageName, final Bounds bounds,
    final HttpServletResponse response) throws ServletException
{
  response.setContentType(getResponseMimeType());
  final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
  try
  {
    writeStream(raster, bounds, Double.NaN, byteStream);
    ServletUtils.writeImageToResponse(response, byteStream.toByteArray());
  }
  catch (final IOException e)
  {
    throw new ServletException("Error writing raster", e);
  }

}

@Override
public void writeToStream(final MrGeoRaster raster, double[] defaults, final ByteArrayOutputStream byteStream)
    throws IOException
{
  // no-op. We need a different set so we can write geotiffs (with header info);
}

private void writeStream(final MrGeoRaster raster, final Bounds bounds, final double nodata,
    final ByteArrayOutputStream byteStream) throws IOException
{
  double nodatas[] = new double[raster.bands()];
  Arrays.fill(nodatas, nodata);

  GDALJavaUtils.saveRaster(raster.toDataset(bounds, nodatas), byteStream, bounds, nodata);

  byteStream.close();
}

}
