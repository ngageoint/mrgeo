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

package org.mrgeo.resources.mrspyramid;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.mrgeo.colorscale.ColorScale;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.services.mrspyramid.MrsPyramidService;
import org.mrgeo.services.mrspyramid.MrsPyramidServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Providers;
import java.util.List;

/**
 * This is a first crack at a resource to represent color scales.  *IT IS BY NO MEANS A FINAL
 * SOLUTION*, and was implemented to support the Capstone UI.  This resource should definitely be
 * modified/replaced to make interaction with color scales more RESTful.
 */
@Path("/colorscales")
public class ColorScaleResource
{
private static final Logger log = LoggerFactory.getLogger(ColorScaleResource.class);


@Context
Providers providers;

@Context
MrsPyramidService service;

/**
 * Recursively returns all MrGeo color scale files in the file system.
 * <p>
 * I was unable to return GenericEntity<List<String>> as JSON, so returning a class with a
 * string list member variable here instead.
 *
 * @return A list of color scale file paths
 * @throws MrsPyramidServiceException If an error occurs while accessing the file system.
 */
@GET
@Produces(MediaType.APPLICATION_JSON)
public ColorScaleList get() throws MrsPyramidServiceException
{
  log.info("Retrieving color scales file list...");

  getService();
  List<String> files = service.getColorScales();

  log.info("Color scales file list retrieved.");

  return new ColorScaleList(files);
}

/**
 * This is a poorly differentiated REST path, it really should have been /swatch to separate
 * it in namespace from the above. As is, only the query param separates these two.
 */
@SuppressFBWarnings(value = "JAXRS_ENDPOINT", justification = "verified")
@GET
@Produces("image/png")
@Path("/{path: .*+}")
public Response getColorScaleSwatch(@PathParam("path") String colorScalePath,
    @DefaultValue("200") @QueryParam("width") int width,
    @DefaultValue("20") @QueryParam("height") int height
)
{
  try
  {
    getService();

    String format = "png";
    MrGeoRaster ri = service.createColorScaleSwatch(colorScalePath, format, width, height);

    return service.getImageResponseWriter(format).write(ri).build();
  }
  catch (MrsPyramidServiceException e)
  {
    log.error("Exception thrown", e);
    return Response.status(Status.BAD_REQUEST).entity("Color scale file not found: " + colorScalePath).build();
  }
}

@SuppressFBWarnings(value = "JAXRS_ENDPOINT", justification = "verified")
@GET
@Produces("text/html")
@Path("/legend/{name: .*+}")
public Response getColorScaleLegend(@PathParam("name") String name,
    @DefaultValue("20") @QueryParam("width") int width,
    @DefaultValue("200") @QueryParam("height") int height,
    @DefaultValue("0") @QueryParam("min") Double min,
    @QueryParam("max") Double max,
    @DefaultValue("") @QueryParam("units") String units
)
{
  try
  {
    getService();

    ColorScale cs = service.getColorScaleFromName(name);
    int fontSize = 12;
    String position;
    int dimension;
    String floatStyle;
    int offset;
    StringBuilder textStyle = new StringBuilder();
    if (width > height)
    {
      String rotate = "90deg";
      String origin = "left top;";
      position = "left";
      dimension = width;
      floatStyle = ""; //"top:" + height + ";";
      //textStyle.append("border: 1px solid red;")
      textStyle.append("white-space: nowrap;")
//                      .append("left:").append(height).append(";")
//                      .append("right:100%;")
//                      .append("height:").append(width).append(";")
//                      .append("-moz-transform: rotate(90.0deg);")  /* FF3.5+ */
//                      .append("-o-transform: rotate(90.0deg);")  /* Opera 10.5 */
//                      .append("-webkit-transform: rotate(90.0deg);")  /* Saf3.1+, Chrome */
//                      .append("filter:  progid:DXImageTransform.Microsoft.BasicImage(rotation=-0.083);")  /* IE6,IE7 */
//                      .append("-ms-filter: \"progid:DXImageTransform.Microsoft.BasicImage(rotation=-0.083)\";") /* IE8 */
          .append("-webkit-transform: rotate(").append(rotate).append(");")
          .append("-webkit-transform-origin: ").append(origin)
          .append("-moz-transform: rotate(").append(rotate).append(");")
          .append("-moz-transform-origin: ").append(origin)
          .append("-ms-transform: rotate(").append(rotate).append(");")
          .append("-ms-transform-origin: ").append(origin)
          .append("-o-transform: rotate(").append(rotate).append(");")
          .append("-o-transform-origin: ").append(origin)
          .append("transform: rotate(").append(rotate).append(");")
          .append("transform-origin: ").append(origin);
      offset = -(fontSize);
    }
    else
    {
      position = "top";
      dimension = height;
      floatStyle = "float:left;";
      offset = (fontSize / 2);
    }

//          String relativePath = "../";
//          for (int i=0; i<StringUtils.countMatches(name, "/"); i++) {
//              relativePath += "../";
//          }

    StringBuilder html = new StringBuilder();

    html.append("<div>");
    html.append("<div style='").append(floatStyle).append(" width:").append(width).append("; height:").append(height)
        .append(";'>")
        .append("<img src='")
//                  .append(relativePath)
        .append(name)
        .append("?width=").append(width).append("&amp;height=").append(height)
        .append("' alt='color scale'/>")
        .append("</div>");
    html.append("<div style='position:relative; font:").append(fontSize).append("px arial,sans-serif; ")
        .append(floatStyle).append(" width:").append(width).append("; height:").append(width).append(";'>");
    //This adds value markers in a linear fashion for the same number of color breaks
    int numBreaks = cs.keySet().size();
    Double deltaValue = (max - min) / numBreaks;
    int deltaPixel = dimension / numBreaks;
    if (units.equalsIgnoreCase("likelihood"))
    {
      String top;
      String bottom;
      if (width > height)
      {
        top = "Lower Likelihood";
        bottom = "Higher Likelihood";
        offset = fontSize;
      }
      else
      {
        top = "Higher Likelihood";
        bottom = "Lower Likelihood";
        offset = (fontSize * 2);
      }

      html.append("<div style='width:").append(deltaPixel).append("; position:absolute; ").append(position)
          .append(":0;'>")
          .append(top)
          .append("</div>")
          .append("<div style='width:").append(deltaPixel).append("; position:absolute; ").append(position).append(":")
          .append(dimension - offset).append(";'>")
          .append(bottom)
          .append("</div>");
    }
    else
    {
      for (int i = 0; i <= numBreaks; i++)
      {
        html.append("<div style='").append(textStyle.toString()).append("position:absolute; ").append(position)
            .append(":").append((deltaPixel * i) - offset).append(";'>");
        if (width > height)
        {
          html.append(service.formatValue(min + (deltaValue * i), units));
        }
        else
        {
          html.append(service.formatValue(max - (deltaValue * i), units));
        }
        html.append("</div>");
      }
    }
    html.append("</div>");
    html.append("</div>");
    return Response.ok()
        .entity(html.toString())
        .header("Content-Type", "text/html")
        .build();
  }
  catch (MrsPyramidServiceException e)
  {
    log.error("Color scale file not found: " + name, e);
    return Response.status(Status.BAD_REQUEST).entity("Color scale not found: " + name).build();
  }
}

private void getService()
{
  if (service == null)
  {
    ContextResolver<MrsPyramidService> resolver =
        providers.getContextResolver(MrsPyramidService.class, MediaType.WILDCARD_TYPE);
    if (resolver != null)
    {
      service = resolver.getContext(MrsPyramidService.class);
    }
  }
}


}
