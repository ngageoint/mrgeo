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

package org.mrgeo.resources.mrspyramid;

import java.awt.image.Raster;
import java.io.IOException;
import java.util.List;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.xml.parsers.ParserConfigurationException;

import org.mrgeo.colorscale.ColorScale;
import org.mrgeo.services.mrspyramid.MrsPyramidService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

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
  MrsPyramidService service;

  /**
   * Recursively returns all MrGeo color scale files in the file system.
   *
   * I was unable to return GenericEntity<List<String>> as JSON, so returning a class with a
   * string list member variable here instead.
   *
   * @return A list of color scale file paths
   * @throws IOException If an error occurs while accessing the file system.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public ColorScaleList get()
    throws SAXException, ParserConfigurationException, Exception
  {
    log.info("Retrieving color scales file list...");

    List<String> files = service.getColorScales();

    log.info("Color scales file list retrieved.");

    return new ColorScaleList(files);
  }

  /**
   * This is a poorly differentiated REST path, it really should have been /swatch to separate
   * it in namespace from the above. As is, only the query param separates these two.
   *
   * @param colorScalePath
   * @param width
   * @param height
   */
  @GET
  @Produces("image/png")
  @Path("/{path: .*+}")
  public Response getColorScaleSwatch(@PathParam("path") String colorScalePath,
          @DefaultValue("200") @QueryParam("width") int width,
          @DefaultValue("20") @QueryParam("height") int height
          )
  {
      try {
        String format = "png";
        Raster ri = service.createColorScaleSwatch(colorScalePath, format, width, height);

        return service.getImageResponseWriter(format).write(ri).build();
      } catch (Exception ex) {
        ex.printStackTrace();
        return Response.status(Status.BAD_REQUEST).entity("Color scale file not found: " + colorScalePath).build();
      }
  }

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
      try {
          ColorScale cs = service.getColorScaleFromName(name);
          int fontSize = 12;
          String position;
          int dimension;
          String floatStyle;
          int offset;
          StringBuilder textStyle = new StringBuilder();
          if (width > height) {
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
          } else {
              position = "top";
              dimension = height;
              floatStyle = "float:left;";
              offset = (fontSize/2);
          }

//          String relativePath = "../";
//          for (int i=0; i<StringUtils.countMatches(name, "/"); i++) {
//              relativePath += "../";
//          }

          StringBuilder html = new StringBuilder();

         html.append("<div>");
         html.append("<div style='").append(floatStyle).append(" width:").append(width).append("; height:").append(height).append(";'>")
                  .append("<img src='")
//                  .append(relativePath)
                  .append(name)
                  .append("?width=").append(width).append("&amp;height=").append(height)
                  .append("' alt='color scale'/>")
                  .append("</div>");
          html.append("<div style='position:relative; font:").append(fontSize).append("px arial,sans-serif; ").append(floatStyle).append(" width:").append(width).append("; height:").append(width).append(";'>");
          //This adds value markers in a linear fashion for the same number of color breaks
          int numBreaks = cs.keySet().size();
          Double deltaValue = (max-min) / numBreaks;
          int deltaPixel = dimension / numBreaks;
          if (units.equalsIgnoreCase("likelihood")) {
              String top;
              String bottom;
              if (width > height) {
                  top = "Lower Likelihood";
                  bottom = "Higher Likelihood";
                  offset = fontSize;
              } else {
                  top = "Higher Likelihood";
                  bottom = "Lower Likelihood";
                  offset = (fontSize*2);
              }

              html.append("<div style='width:").append(deltaPixel).append("; position:absolute; ").append(position).append(":0;'>")
              .append(top)
              .append("</div>")
              .append("<div style='width:").append(deltaPixel).append("; position:absolute; ").append(position).append(":").append(dimension-offset).append(";'>")
              .append(bottom)
              .append("</div>");
          } else {
              for (int i=0; i<=numBreaks; i++) {
                   html.append("<div style='").append(textStyle.toString()).append("position:absolute; ").append(position).append(":").append((deltaPixel * i) - offset).append(";'>");
                   if (width > height) {
                       html.append(service.formatValue(min + (deltaValue * i), units));
                   } else {
                       html.append(service.formatValue(max - (deltaValue * i), units));
                   }
                   html.append("</div>");
              }
          }
          html.append("</div>");
          html.append("</div>");
        return Response.ok(html.toString()).header("Content-Type", "text/html").build();
      } catch (Exception ex) {
        log.error("Color scale file not found: " + name, ex);
        return Response.status(Status.BAD_REQUEST).entity("Color scale not found: " + name).build();
      }
  }
}
