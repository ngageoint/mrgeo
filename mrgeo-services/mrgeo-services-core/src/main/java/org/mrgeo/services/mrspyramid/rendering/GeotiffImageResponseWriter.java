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

package org.mrgeo.services.mrspyramid.rendering;

import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.services.ServletUtils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.GDALUtils;
import org.mrgeo.utils.TMSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.awt.image.Raster;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class GeotiffImageResponseWriter extends TiffImageResponseWriter
{
  private static final Logger log = LoggerFactory.getLogger(GeotiffImageResponseWriter.class);

  public GeotiffImageResponseWriter()
  {
  }

  @Override
  public String[] getMimeTypes()
  {
    return new String[] { "image/geotiff", "image/geotif" };
  }

  @Override
  public String getResponseMimeType()
  {
    return "image/geotiff";
  }

  @Override
  public String[] getWmsFormats()
  {
    return new String[] { "geotiff", "geotif" };
  }

  @Override
  public Response.ResponseBuilder write(final Raster raster)
  {
    try
    {
      final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

      writeStream(raster, Bounds.world, Double.NaN, byteStream);

      return Response.ok(byteStream.toByteArray(), getResponseMimeType()).header("Content-type",
        getResponseMimeType()).header("Content-Disposition", "attachment; filename=image.tif");
    }
    catch (final Exception e)
    {
      if (e.getMessage() != null)
      {
        return Response.serverError().entity(e.getMessage());
      }
      return Response.serverError().entity("Internal Error");

    }

  }

  @Override
  public void write(final Raster raster, final HttpServletResponse response)
    throws ServletException
  {
    response.setContentType(getResponseMimeType());
    final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    try
    {
      writeStream(raster, Bounds.world, Double.NaN, byteStream);
      ServletUtils.writeImageToResponse(response, byteStream.toByteArray());
    }
    catch (final IOException e)
    {
      throw new ServletException("Error writing raster");
    }
  }

  @Override
  public Response.ResponseBuilder write(final Raster raster, final int tileColumn, final int tileRow,
    final double scale, final MrsImagePyramid pyramid)
  {
    try
    {
      final int tilesize = pyramid.getMetadata().getTilesize();

      final int zoom = TMSUtils.zoomForPixelSize(scale, tilesize);
      final TMSUtils.Bounds bounds = TMSUtils.tileBounds(tileColumn, tileRow, zoom, tilesize);
      final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

      writeStream(raster, bounds.asBounds(), pyramid.getMetadata().getDefaultValue(0), byteStream);

      return Response.ok(byteStream.toByteArray(), getResponseMimeType()).header("Content-type",
        getResponseMimeType()).header("Content-Disposition",
                                      "attachment; filename=" + pyramid.getName() + ".tif");
    }
    catch (final Exception e)
    {
      if (e.getMessage() != null)
      {
        return Response.serverError().entity(e.getMessage());
      }
      return Response.serverError().entity("Internal Error");

    }

  }

  @Override
  public void write(final Raster raster, final int tileColumn, final int tileRow,
    final double scale, final MrsImagePyramid pyramid, final HttpServletResponse response)
    throws ServletException
  {
    try
    {
    final int tilesize = pyramid.getMetadata().getTilesize();

    final int zoom = TMSUtils.zoomForPixelSize(scale, tilesize);
    final TMSUtils.Bounds bounds = TMSUtils.tileBounds(tileColumn, tileRow, zoom, tilesize);

    response.setContentType(getResponseMimeType());
    final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      writeStream(raster, bounds.asBounds(), pyramid.getMetadata().getDefaultValue(0), byteStream);
      ServletUtils.writeImageToResponse(response, byteStream.toByteArray());
    }
    catch (final IOException e)
    {
      throw new ServletException("Error writing raster");
    }

  }

  /**
   * 
   * @param imageName
   * @param bounds
   * @return
   * @throws IOException
   */
  @Override
  public Response.ResponseBuilder write(final Raster raster, final String imageName, final Bounds bounds)
  {
    try
    {
      final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

      MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(imageName, DataProviderFactory.AccessMode.READ,
          (ProviderProperties) null);
      MrsImagePyramidMetadata metadata = dp.getMetadataReader().read();

      writeStream(raster, bounds, metadata.getDefaultValue(0), byteStream);

      return Response.ok(byteStream.toByteArray(), getResponseMimeType()).header("Content-type",
        getResponseMimeType()).header("Content-Disposition",
                                      "attachment; filename=" + imageName + ".tif");
    }
    catch (final Exception e)
    {
      if (e.getMessage() != null)
      {
        return Response.serverError().entity(e.getMessage());
      }
      return Response.serverError().entity("Internal Error");

    }

  }

  @Override
  public void write(final Raster raster, final String imageName, final Bounds bounds,
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
      throw new ServletException("Error writing raster");
    }

  }

  @Override
  public void writeToStream(final Raster raster, double[] defaults, final ByteArrayOutputStream byteStream)
    throws IOException
  {
    // no-op. We need a different set so we can write geotiffs (with header info);
  }

  private void writeStream(final Raster raster, final Bounds bounds, final double nodata,
    final ByteArrayOutputStream byteStream) throws IOException
  {
    GDALUtils.saveRaster(raster, byteStream, bounds, nodata);

    byteStream.close();
  }

}
