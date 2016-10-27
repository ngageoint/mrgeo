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

package org.mrgeo.resources.mrspyramid;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.mrgeo.colorscale.ColorScale;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.mapalgebra.MapAlgebraJob;
import org.mrgeo.services.SecurityUtils;
import org.mrgeo.services.mrspyramid.MrsPyramidService;
import org.mrgeo.services.mrspyramid.rendering.ImageRenderer;
import org.mrgeo.services.mrspyramid.rendering.TiffImageRenderer;
import org.mrgeo.utils.tms.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import java.io.FileNotFoundException;

//import org.mrgeo.services.mrspyramid.MrsPyramidService;

@Path("/raster")
public class RasterResource
{
@Context
UriInfo uriInfo;

@Context
HttpServletRequest request;

@Context
MrsPyramidService service;

private static final String TIFF_MIME_TYPE = "image/tiff";
private static final String KML_INPUT_FORMAT = "kml";

private static final Logger log = LoggerFactory.getLogger(RasterResource.class);

/*
 * Accepts a MapAlgebra expression and runs a job that will create a raster as
 * a result of of running the expression.
 *
 * @param output - unique id, this will be the name of the output raster
 *
 * @param expression - mapalgebra expression
 *
 * @param basepath [optional] - this is added for testing purposes. This will
 * be the path where the output raster will be created.
 */
@SuppressFBWarnings(value = "JAXRS_ENDPOINT", justification = "verified")
@PUT
@Path("/{output}/mapalgebra/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.TEXT_PLAIN)
public Response createMapAlgebraJob(@PathParam("output") String outputId,
    @QueryParam("basePath") String basePath,
    @QueryParam("protectionLevel") @DefaultValue("") String protectionLevel,
    String expression)
{
  try
  {
    // TODO: After MrsPyramid 2.0 is complete, we will no longer specify a
    // full path but instead just the resource name. This is because there is no concept of
    // paths in Accumulo.
//      String outputPathStr = service.getOutputImageStr(basePath, outputId);
    // TODO: Need to construct provider properties from the WebRequest using
    // a new security layer and pass those properties to MapAlgebraJob.
    MapAlgebraJob job = new MapAlgebraJob(expression, outputId,
        protectionLevel, SecurityUtils.getProviderProperties());
    service.getJobManager().submitJob("MapAlgebra job " + outputId, job);
//      long jobId = service.getJobManager().submitJob("MapAlgebra job " + outputId, job);
//      String jobUri = uriInfo.getBaseUri().toString() + "job/";
//      jobUri = HttpUtil.updateSchemeFromHeaders(jobUri, request);
    // TODO: Revisit the response whenever we re-think how job status reporting
    // will work within Spark.
//      JobInfoResponse jr = JobResponseFormatter.createJobResponse(service.getJobManager().getJob(jobId), jobUri);
//      return Response.status(Status.ACCEPTED).entity(jr).build();
    return Response.status(Status.ACCEPTED).build();
  } catch (Exception e) {
    throw new WebApplicationException(
        Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build() );
  }
}

  /*
   * Accepts a GeoTiff stream for in-memory ingest to MrsPyramid.
   *
   * @param output - unique id, this will be the name of the ingested raster
   *
   */
//  @POST
//  @Path("/{output}/ingest/")
//  @Produces(MediaType.APPLICATION_JSON)
//  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
//  public Response ingestRaster(@PathParam("output") String output,
//      @QueryParam("protectionLevel") @DefaultValue("") String protectionLevel)
//  {
//    try {
//      // TODO: Need to construct provider properties from the WebRequest using
//      // a new security layer and pass those properties to MapAlgebraJob.
//      String pyramidOutput = service.ingestImage(request.getInputStream(), output,
//          protectionLevel, SecurityUtils.getProviderProperties());
//      //TODO: write a metadata record to catalog??
////          String url = request.getRequestURI().substring(request.getContextPath().length());
////          URI uri = new URI(url);
//      String createdDate = new DateTime(DateTimeZone.UTC).toString();
//      String json = new JSONStringer()
//          .object()
//          .key("path").value( pyramidOutput )
//              //.key("uri").value( uri )
//          .key("created_date").value( createdDate )
//          .endObject()
//          .toString();
//      return Response.ok().entity( json ).build();
//    } catch (IOException ioe) {
//      log.error("Error reading POST content", ioe);
//      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type("text/plain").entity("Error reading POST content").build();
//    } catch (IllegalStateException e) {
//      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type("text/plain").entity("Output path already exists").build();
//    } catch (URISyntaxException e) {
//      log.error("Error creating pyramid URI", e);
//      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type("text/plain").entity("Error creating pyramid URI").build();
//    } catch (JSONException e) {
//      log.error("Error creating JSON response", e);
//      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type("text/plain").entity("Error creating JSON response").build();
//    } catch (Exception e) {
//      log.error("Error ingesting raster", e);
//      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).type("text/plain").entity("Error ingesting raster").build();
//    }
//  }

@SuppressFBWarnings(value = "JAXRS_ENDPOINT", justification = "verified")
@GET
@Produces("image/*")
@Path("/{output: .*+}")
public Response getImage(@PathParam("output") String imgName,
    @QueryParam("format") @DefaultValue("png") String format,
    @QueryParam("bbox") @DefaultValue("-180, -90, 180, 90") String bbox,
    @QueryParam("width") @DefaultValue("600") int width,
    @QueryParam("height") @DefaultValue("400") int height,
    @QueryParam("color-scale-name") String colorScaleName,
    @QueryParam("color-scale") String colorScale,
    @QueryParam("min") Double min,
    @QueryParam("max") Double max,
    @QueryParam("srs") String srs,
    @QueryParam("zoom-level") @DefaultValue("-1") int zoomLevel)
{
  String error;
  try
  {
    String[] bBoxValues = bbox.split(",");
    if (bBoxValues.length != 4)
    {
      return Response.status(Status.BAD_REQUEST)
          .entity("Bounding box must have four comma delimited arguments.").build();
    }
    double minX = Double.valueOf(bBoxValues[0]);
    double minY = Double.valueOf(bBoxValues[1]);
    double maxX = Double.valueOf(bBoxValues[2]);
    double maxY = Double.valueOf(bBoxValues[3]);

    Bounds bounds = new Bounds(minX, minY, maxX, maxY);

    ColorScale cs = null;
    try
    {
      if (colorScaleName != null)
      {
        if (colorScale != null)
        {
          return Response.status(Status.BAD_REQUEST)
              .entity("Only one of ColorScale or ColorScaleName can be specified.").build();
        }
        cs = service.getColorScaleFromName(colorScaleName);
      }
      else if (colorScale != null)
      {
        cs = service.getColorScaleFromJSON(colorScale);
      }
//        else
//        {
//          cs = service.getColorScaleFromPyramid(imgName);
//        }
    }
    catch (Exception e)
    {
      return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
    }

    if ( zoomLevel != -1 )
    {
      MrsPyramid pyramid = service.getPyramid(imgName, SecurityUtils.getProviderProperties());
      if (pyramid == null)
      {
        return Response.status(Status.NOT_FOUND).entity(imgName + " not found").build();
      }
      // Throw an error if the requested zoom level does not exist
      if (pyramid.getMetadata().getName(zoomLevel) == null)
      {
        return Response.status(Status.BAD_REQUEST).entity("Invalid zoom level specified.")
            .build();
      }
    }

    //for kml we'll use the original raster resource code path, since no kml generation exists
    //in the original wms code
    if (!format.equals(KML_INPUT_FORMAT))
    {
      MrsPyramid pyramid = service.getPyramid(imgName, SecurityUtils.getProviderProperties());
      if (pyramid == null)
      {
        return Response.status(Status.NOT_FOUND).entity(imgName + " not found").build();
      }
      if (!bounds.toEnvelope().intersects(pyramid.getBounds().toEnvelope()))
      {
        log.debug("request bounds does not intersects image bounds");
        byte imageData[] = service.getEmptyTile(width, height, format);
        String type = service.getContentType(format);
        return Response.ok(imageData).header("Content-Type", type).build();
      }
      ImageRenderer renderer;
      try
      {
        renderer = service.getImageRenderer(format);
      }
      catch (IllegalArgumentException e)
      {
        if (e.getMessage().toUpperCase().contains("INVALID FORMAT"))
        {
          return Response.status(Status.BAD_REQUEST).entity(
              "Unsupported image format - " + format).build();
        }
        throw e;
      }
      // TODO: Need to construct provider properties from the WebRequest using
      // a new security layer and pass those properties.
      MrGeoRaster result = renderer.renderImage(imgName, bounds, width, height,
          SecurityUtils.getProviderProperties(), srs);

      if (!(renderer instanceof TiffImageRenderer))
      {
        log.debug("Applying color scale to image " + imgName + " ...");
        //Add min/max colorscale override
        double[] overrideExtrema = renderer.getExtrema();
        if (min != null) overrideExtrema[0] = min;
        if (max != null) overrideExtrema[1] = max;
        result = service.applyColorScaleToImage(format, result, cs, renderer, overrideExtrema);
        log.debug("Color scale applied to image " + imgName);
      }
      return service.getImageResponseWriter(format).write(result, imgName, bounds).build();
    }
    else
    {
      // Render KML
      return service.renderKml(imgName, bounds, width, height, cs, zoomLevel,
          SecurityUtils.getProviderProperties());
    }
  }
  catch (FileNotFoundException fnfe) {
    return Response.status(Status.NOT_FOUND).entity(fnfe.getMessage()).build();
  }
  catch (Exception e)
  {
    error = e.getMessage();
    log.error("Unable to retrieve image " + e.getMessage(), e);
  }
  return Response.serverError().entity(error).build();
}

}
