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

import org.mrgeo.image.MrsPyramid;
import org.mrgeo.services.ServletUtils;
import org.mrgeo.utils.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.awt.image.Raster;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Base class for WMS image response writers; Each image format should subclass this.
 */
public abstract class ImageResponseWriterAbstract implements ImageResponseWriter
{
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(ImageResponseWriterAbstract.class);

  @Override
  public Response.ResponseBuilder write(final Raster raster)
  {
    return write(raster, (double[])(null));
  }

  @Override
  public Response.ResponseBuilder write(final Raster raster, double[] defaults)
  {
    try
    {
      final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

      writeToStream(raster, defaults, byteStream);

      // TODO: the type passed to Response.ok may not be correct here
      return Response.ok(byteStream.toByteArray()).header("Content-type", getResponseMimeType());
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
  public void write(final Raster raster, final HttpServletResponse response) throws ServletException
  {
    write(raster, null, response);
  }

  @Override
  public void write(final Raster raster, double[] defaults, final HttpServletResponse response) throws ServletException
  {
    response.setContentType(getResponseMimeType());
    final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    try
    {
      writeToStream(raster, defaults, byteStream);
      ServletUtils.writeImageToResponse(response, byteStream.toByteArray());
    }
    catch (IOException e)
    {
      throw new ServletException("Error writing raster");
    }
  }

  @Override
  public Response.ResponseBuilder write(final Raster raster, final int tileColumn, final int tileRow,
    final double scale, final MrsPyramid pyramid) throws IOException
  {
    return write(raster, pyramid.getMetadata().getDefaultValues());
  }

  @Override
  public void write(final Raster raster, final int tileColumn, final int tileRow,
                    final double scale, final MrsPyramid pyramid, final HttpServletResponse response)
      throws ServletException, IOException
  {
    write(raster, pyramid.getMetadata().getDefaultValues(), response);
    }

  @Override
  public Response.ResponseBuilder write(final Raster raster, final String imageName, final Bounds bounds)
  {
    return write(raster);
  }

  @Override
  public void write(final Raster raster, final String imageName, final Bounds bounds,
    final HttpServletResponse response) throws ServletException
    {
    write(raster, response);
    }

}
