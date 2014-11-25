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

package org.mrgeo.services.mrspyramid.rendering;

import org.mrgeo.rasterops.ColorScale;
import org.mrgeo.utils.Bounds;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import javax.activation.MimetypesFileTypeMap;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.Properties;

/**
 * Builds an HTTP response for KML data
 */
public class KmlResponseBuilder implements RasterResponseBuilder
{
  CoordinateReferenceSystem coordSys;

  public KmlResponseBuilder(final CoordinateReferenceSystem crs)
  {
    coordSys = crs;
  }

  @Override
  public String getFormatSuffix()
  {
    return "kml";
  }

  @Override
  public String getMimeType()
  {
    return "application/vnd.google-earth.kml+xml";
  }

  @Override
  public Response getResponse(final String pyrName, final Bounds bounds, final int width,
    final int height, final ColorScale cs, final String reqUrl, final int zoomLevel,
    final Properties providerProperties)
  {
    try
    {
      final String kmlBody = ImageRendererAbstract
        .asKml(pyrName, bounds, width, height, cs, reqUrl, providerProperties);
      final String type = new MimetypesFileTypeMap().getContentType(getFormatSuffix());
      final String headerInfo = "attachment; filename=" + pyrName + "." + getFormatSuffix();
      return Response.ok(kmlBody, type).header("Content-Disposition", headerInfo).header(
        "Content-type", getMimeType()).build();
    }
    catch (final IOException e)
    {
      if (e.getMessage() != null)
      {
        return Response.serverError().entity(e.getMessage()).build();
      }
      return Response.serverError().entity("Internal Error").build();
    }
    catch (final Exception e)
    {
      if (e.getMessage() != null)
      {
        return Response.serverError().entity(e.getMessage()).build();
      }
      return Response.serverError().entity("Internal Error").build();
    }
  }
}
