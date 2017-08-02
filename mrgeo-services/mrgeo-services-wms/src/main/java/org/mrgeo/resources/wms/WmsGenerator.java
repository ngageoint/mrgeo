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

package org.mrgeo.resources.wms;

import org.apache.commons.lang3.StringUtils;
import org.mrgeo.colorscale.ColorScale;
import org.mrgeo.colorscale.ColorScaleManager;
import org.mrgeo.colorscale.applier.ColorScaleApplier;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.data.raster.MrGeoRaster;
import org.mrgeo.image.ImageStats;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.services.SecurityUtils;
import org.mrgeo.services.Version;
import org.mrgeo.services.mrspyramid.MrsPyramidService;
import org.mrgeo.services.mrspyramid.MrsPyramidServiceException;
import org.mrgeo.services.mrspyramid.rendering.*;
import org.mrgeo.services.utils.RequestUtils;
import org.mrgeo.utils.XmlUtils;
import org.mrgeo.utils.tms.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.CDATASection;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.dom.DOMSource;
import java.awt.image.DataBuffer;
import java.io.IOException;
import java.util.List;
import java.util.Map;

//import javax.servlet.http.HttpServlet;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;

//import org.mrgeo.utils.logging.LoggingUtils;

@Path("/wms")
public class WmsGenerator
{
static final String JPEG_MIME_TYPE = "image/jpeg";
static final String PNG_MIME_TYPE = "image/png";

//  private static Path basePath = null;
//  private static Path colorScaleBasePath = null;
// private static CoordinateReferenceSystem coordSys = null;
static final String TIFF_MIME_TYPE = "image/tiff";
private static final Logger log = LoggerFactory.getLogger(WmsGenerator.class);
private static final String WMS_VERSION = "1.3.0";
private static final String WMS_SERVICE = "wms";
private Version version = new Version(WMS_VERSION);

public WmsGenerator()
{
}


private static MrGeoRaster colorRaster(String layer, String style, String imageFormat, ImageRenderer renderer,
    MrGeoRaster result, ProviderProperties providerProperties) throws WmsGeneratorException
{
  try
  {
    if (!(renderer instanceof TiffImageRenderer))
    {
      log.debug("Applying color scale to image {} ...", layer);

      ColorScale cs;
      if (style != null && !style.equalsIgnoreCase("default"))
      {
        cs = ColorScaleManager.fromName(style);
        if (cs == null)
        {
          throw new IOException("Can not load style: " + style);
        }
      }
      else
      {
        MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(layer, DataProviderFactory.AccessMode.READ, providerProperties);
        MrsPyramidMetadata meta = dp.getMetadataReader().read();

        String csname = meta.getTag(MrGeoConstants.MRGEO_DEFAULT_COLORSCALE);
        if (csname != null)
        {
          cs = ColorScaleManager.fromName(csname);
          if (cs == null)
          {
            throw new IOException("Can not load default style: "  + csname);
          }
        }
        else
        {
          cs = ColorScale.createDefaultGrayScale();
        }
      }
      result = ((ColorScaleApplier) ImageHandlerFactory.getHandler(imageFormat,
          ColorScaleApplier.class)).applyColorScale(result, cs, renderer.getExtrema(),
          renderer.getDefaultValues(), renderer.getQuantiles());
      log.debug("Color scale applied to image {}", layer);
    }

    return result;
  }
  catch (IllegalAccessException | IOException | ColorScale.ColorScaleException | InstantiationException e)
  {
    throw new WmsGeneratorException(e);
  }
}


@GET
public Response doGet(@Context UriInfo uriInfo, @Context HttpHeaders headers)
{
  return handleRequest(uriInfo, headers);
}

@POST
public Response doPost(@Context UriInfo uriInfo, @Context HttpHeaders headers)
{
  return handleRequest(uriInfo, headers);
}

/**
 * Returns the value for the specified paramName case-insensitively. If the
 * parameter does not exist, it returns null.
 */
private String getQueryParam(MultivaluedMap<String, String> allParams, String paramName)
{
  for (Map.Entry<String, List<String>> es : allParams.entrySet())
  {
    if (es.getKey().equalsIgnoreCase(paramName))
    {
      if (es.getValue().size() == 1)
      {
        return es.getValue().get(0);
      }
    }
  }

  return null;
}

/**
 * Returns the value for the specified paramName case-insensitively. If the
 * parameter does not exist, it returns defaultValue.
 */
private String getQueryParam(MultivaluedMap<String, String> allParams,
    String paramName,
    String defaultValue)
{
  String value = getQueryParam(allParams, paramName);
  if (value != null)
  {
    return value;
  }
  return defaultValue;
}

private boolean paramExists(MultivaluedMap<String, String> allParams,
    String paramName)
{

  for (Map.Entry<String, List<String>> es : allParams.entrySet())
  {
    if (es.getKey().equalsIgnoreCase(paramName))
    {
      return (es.getValue().size() > 0);
    }
  }

  return false;
}


/**
 * Returns the int value for the specified paramName case-insensitively. If
 * the parameter value exists, but is not an int, it throws a NumberFormatException.
 * If it does not exist, it returns defaultValue.
 */
private int getQueryParamAsInt(MultivaluedMap<String, String> allParams,
    String paramName,
    int defaultValue)
    throws NumberFormatException
{
  for (Map.Entry<String, List<String>> es : allParams.entrySet())
  {
    if (es.getKey().equalsIgnoreCase(paramName))
    {
      if (es.getValue().size() == 1)
      {
        return Integer.parseInt(es.getValue().get(0));
      }
    }
  }
  return defaultValue;
}

/**
 * Returns the int value for the specified paramName case-insensitively. If
 * the parameter value exists, but is not an int, it throws a NumberFormatException.
 * If it does not exist, it returns defaultValue.
 */
private double getQueryParamAsDouble(MultivaluedMap<String, String> allParams,
    String paramName,
    double defaultValue)
    throws NumberFormatException
{
  for (Map.Entry<String, List<String>> es : allParams.entrySet())
  {
    if (es.getKey().equalsIgnoreCase(paramName))
    {
      if (es.getValue().size() == 1)
      {
        return Double.parseDouble(es.getValue().get(0));
      }
    }
  }
  return defaultValue;
}

private Response handleRequest(@Context UriInfo uriInfo, @Context HttpHeaders headers)
{
  long start = System.currentTimeMillis();

  String uri = RequestUtils.buildBaseURI(uriInfo, headers);

  try
  {
    MultivaluedMap<String, String> allParams = uriInfo.getQueryParameters();
    String request = getQueryParam(allParams, "request", "GetCapabilities");
    ProviderProperties providerProperties = SecurityUtils.getProviderProperties();

    String serviceName = getQueryParam(allParams, "service");
    if (serviceName == null)
    {
      return writeError(Response.Status.BAD_REQUEST, "Missing required SERVICE parameter. Should be set to \"WMS\"");
    }
    if (!serviceName.equalsIgnoreCase("wms"))
    {
      return writeError(Response.Status.BAD_REQUEST, "Invalid SERVICE parameter. Should be set to \"WMS\"");
    }

    if (request.equalsIgnoreCase("getmap"))
    {
      return getMap(allParams, providerProperties);
    }
    else if (request.equalsIgnoreCase("getmosaic"))
    {
      return getMosaic(allParams, providerProperties);
    }
    else if (request.equalsIgnoreCase("gettile"))
    {
      return getTile(allParams, providerProperties);
    }
    else if (request.equalsIgnoreCase("getcapabilities"))
    {
      return getCapabilities(uri, allParams, providerProperties);
    }
    else if (request.equalsIgnoreCase("describetiles"))
    {
      return describeTiles(uri, allParams, providerProperties);
    }
    else if (request.equalsIgnoreCase("getlegendgraphic"))
    {
      return getLegend(allParams, providerProperties);
    }

    return writeError(Response.Status.BAD_REQUEST, "Invalid request");
  }
  finally
  {
    if (log.isDebugEnabled())
    {
      log.debug("WMS request time: {}ms", (System.currentTimeMillis() - start));
      // this can be resource intensive.
      System.gc();
      final Runtime rt = Runtime.getRuntime();
      log.debug(String.format("WMS request memory: %.1fMB / %.1fMB%n", (rt.totalMemory() - rt
          .freeMemory()) / 1e6, rt.maxMemory() / 1e6));
    }
  }
}

private boolean isCacheOff(MultivaluedMap<String, String> allParams)
{
  String cacheValue = getQueryParam(allParams, "cache", "");
  return (!StringUtils.isEmpty(cacheValue) && cacheValue.equalsIgnoreCase("off"));
}

private Response.ResponseBuilder setupCaching(Response.ResponseBuilder builder,
    MultivaluedMap<String, String> allParams)
{
  boolean cacheOff = isCacheOff(allParams);
  CacheControl cacheControl = new CacheControl();
  if (cacheOff)
  {
    cacheControl.setNoStore(true);
    cacheControl.setNoCache(true);
    // This is retained from the original WmsGenerator, but it seems wrong.
    //return builder.cacheControl(cacheControl).expires(DateTime.now().toDate());
    return builder.cacheControl(cacheControl);
  }
  else
  {
//      DateTime future = DateTime.now().plusSeconds(3600);

    cacheControl.setMaxAge(3600);
    cacheControl.setNoCache(false);

    // This is retained from the original WmsGenerator, but it seems wrong.
    //return builder.cacheControl(cacheControl).expires(future.toDate());
    return builder.cacheControl(cacheControl);
  }
}

// Inspired by: http://docs.geoserver.org/stable/en/user/services/wms/get_legend_graphic/index.html
private Response getLegend(MultivaluedMap<String, String> allParams, ProviderProperties providerProperties)
{
  // Get all of the query parameter values needed and validate them
//  String layer = getQueryParam(allParams, "layer");
//
//  if (layer == null || layer.isEmpty())
//  {
//    return writeError(Response.Status.BAD_REQUEST, "Missing required LAYER parameter");
//  }

  String format = getQueryParam(allParams, "format", "png");
  String colorscalename = getQueryParam(allParams, "style", null);


  int width = getQueryParamAsInt(allParams, "width", 72);
  int height = getQueryParamAsInt(allParams, "height", 72);

  MrsPyramidService service = new MrsPyramidService();

//  double[] extrema;
//  try
//  {
//    MrsPyramid image = service.getPyramid(layer, providerProperties);
//    if (stats != null)
//    {
//      extrema = new double[]{stats.min, stats.max};
//    }
//    else
//    {
//      extrema = new double[]{0, width > height ? width : height};
//    }
//  }
//  catch (MrsPyramidServiceException e)
//  {
//    return writeError(Response.Status.BAD_REQUEST, "Can not load LAYER");
//  }

  ColorScale cs;
  try
  {
    if (colorscalename != null)
    {
      cs = service.getColorScaleFromName(colorscalename);
    }
    else
    {
      cs = ColorScale.createDefault();

    }
  }
  catch (MrsPyramidServiceException e)
  {
    return writeError(Response.Status.INTERNAL_SERVER_ERROR, "Can not load STYLE");
  }

  try
  {

    MrGeoRaster swatch = service.createColorScaleSwatch(cs, format, width, height, cs.getScaleMin(), cs.getScaleMax());

    // Bounds bounds = new Bounds(0, 0, width, height);

    Response.ResponseBuilder builder = ((ImageResponseWriter) ImageHandlerFactory
        .getHandler(format, ImageResponseWriter.class))
        .write(swatch);

    return setupCaching(builder, allParams).build();
  }
  catch (MrsPyramidServiceException | IllegalAccessException | InstantiationException e)
  {
    return writeError(Response.Status.INTERNAL_SERVER_ERROR, "Error creating legend");
  }
}

private Response getMap(MultivaluedMap<String, String> allParams, ProviderProperties providerProperties)
{
  // Get all of the query parameter values needed and validate them
  String layers = getQueryParam(allParams, "layers");
  String[] layerNames = null;
  if (layers != null && !layers.isEmpty())
  {
    layerNames = layers.split(",");
  }
  if (layerNames == null || layerNames.length == 0)
  {
    return writeError(Response.Status.BAD_REQUEST, "Missing required LAYERS parameter");
  }
  if (layerNames.length > 1)
  {
    return writeError(Response.Status.BAD_REQUEST, "Only one LAYER is supported");
  }
  String styles = getQueryParam(allParams, "styles");
  String styleNames[] = null;
  if (styles != null && !styles.isEmpty())
  {
    styleNames = styles.split(",");
//      if (styleNames.length != layerNames.length)
//      {
//        return writeError(Response.Status.BAD_REQUEST, "There are a different number of LAYERS (" + layerNames.length + ") than STYLES(" + styleNames.length + ")");
//      }
  }
//    else
//    {
//      return writeError(Response.Status.BAD_REQUEST, "Missing required STYLES parameter");
//    }
  String srs;
  try
  {
    srs = getSrsParam(allParams);
  }
  catch (Exception e)
  {
    log.error("Exception thrown", e);
    return writeError(Response.Status.BAD_REQUEST, e.getMessage());
  }
  Bounds bounds;
  try
  {
    bounds = getBoundsParam(allParams, "bbox");
  }
  catch (Exception e)
  {
    log.error("Exception thrown", e);
    return writeError(Response.Status.BAD_REQUEST, e.getMessage());
  }
  String format = getQueryParam(allParams, "format");
  if (format == null)
  {
    return writeError(Response.Status.BAD_REQUEST, "Missing required FORMAT parameter");
  }
  if (!paramExists(allParams, "width"))
  {
    return writeError(Response.Status.BAD_REQUEST, "Missing required WIDTH parameter");
  }
  int width = getQueryParamAsInt(allParams, "width", 0);
  if (!paramExists(allParams, "height"))
  {
    return writeError(Response.Status.BAD_REQUEST, "Missing required HEIGHT parameter");
  }
  int height = getQueryParamAsInt(allParams, "height", 0);

  ImageRenderer renderer;
  try
  {
    renderer = (ImageRenderer) ImageHandlerFactory.getHandler(format, ImageRenderer.class);
  }
  catch (Exception e)
  {
    log.error("Exception thrown", e);
    return writeError(Response.Status.BAD_REQUEST, e.getMessage());
  }

  // Return the resulting image
  try
  {
    MrGeoRaster result = renderer.renderImage(layerNames[0], bounds, width, height, providerProperties, srs);

    result = colorRaster(layerNames[0],
        (styleNames != null && styleNames.length > 0) ? styleNames[0] : null,
        format, renderer, result, providerProperties);

    Response.ResponseBuilder builder = ((ImageResponseWriter) ImageHandlerFactory
        .getHandler(format, ImageResponseWriter.class))
        .write(result, layerNames[0], bounds);
    return setupCaching(builder, allParams).build();
  }
  catch (ImageRendererException e) {
    log.error("Unable to render the image in getMap", e);
    return writeError(Response.Status.BAD_REQUEST, "Unable to render the image in getMap");
  }
  catch (Throwable e) {
    log.error("Unable to render the image in getMap", e);
    return writeError(Response.Status.INTERNAL_SERVER_ERROR, "Unable to render the image in getMap");
  }
}

/**
 * Gets the value for the paramName from allParams. If the value does not exist, it
 * throws an exception. It then parses the minX, minY, maxX and maxY settings from
 * the value. If any of the settings are not valid or missing, it throws an
 * exception. If all validation passes, it returns a Bounds object configured with
 * those settings.
 */
private Bounds getBoundsParam(MultivaluedMap<String, String> allParams, String paramName) throws WmsGeneratorException
{
  String bbox = getQueryParam(allParams, paramName);
  if (bbox == null)
  {
    throw new WmsGeneratorException("Missing required BBOX parameter");
  }
  String[] bboxComponents = bbox.split(",");
  if (bboxComponents.length != 4)
  {
    throw new WmsGeneratorException("Invalid BBOX parameter. Should contain minX, minY, maxX, maxY");
  }
  double[] bboxValues = new double[4];
  for (int index = 0; index < bboxComponents.length; index++)
  {
    try
    {
      bboxValues[index] = Double.parseDouble(bboxComponents[index]);
    }
    catch (NumberFormatException nfe)
    {
      throw new WmsGeneratorException("Invalid BBOX value: " + bboxComponents[index]);
    }
  }
  return new Bounds(bboxValues[0], bboxValues[1], bboxValues[2], bboxValues[3]);
}

private String getSrsParam(MultivaluedMap<String, String> allParams)
{
  String srs = getQueryParam(allParams, "srs");
  if (srs == null || srs.isEmpty())
  {
    String crs = getQueryParam(allParams, "crs");
    if (crs == null || crs.isEmpty())
    {
      return null;
    }
    else
    {
      return crs;
    }
  }
  else
  {
    return srs;
  }
}

/*
 * GetMosaic implementation
 */
private Response getMosaic(MultivaluedMap<String, String> allParams, ProviderProperties providerProperties)
{
  String layers = getQueryParam(allParams, "layers");
  String[] layerNames = null;
  if (layers != null && !layers.isEmpty())
  {
    layerNames = layers.split(",");
  }
  if (layerNames == null || layerNames.length == 0)
  {
    return writeError(Response.Status.BAD_REQUEST, "Missing required LAYERS parameter");
  }
  if (layerNames.length > 1)
  {
    return writeError(Response.Status.BAD_REQUEST, "Only one LAYER is supported");
  }
  final Bounds bounds;
  try
  {
    bounds = getBoundsParam(allParams, "bbox");
  }
  catch (Exception e)
  {
    log.error("Exception thrown", e);
    return writeError(Response.Status.BAD_REQUEST, e.getMessage());
  }
  String styles = getQueryParam(allParams, "styles");
  String styleNames[] = null;
  if (styles != null && !styles.isEmpty())
  {
    styleNames = styles.split(",");
//      if (styleNames.length != layerNames.length)
//      {
//        return writeError(Response.Status.BAD_REQUEST, "There are a different number of LAYERS (" + layerNames.length + ") than STYLES(" + styleNames.length + ")");
//      }
  }
//    else
//    {
//      return writeError(Response.Status.BAD_REQUEST, "Missing required STYLES parameter");
//    }
  final String srs;
  try
  {
    srs = getSrsParam(allParams);
  }
  catch (Exception e)
  {
    log.error("Exception thrown", e);
    return writeError(Response.Status.BAD_REQUEST, e.getMessage());
  }
  String format = getQueryParam(allParams, "format");
  if (format == null)
  {
    return writeError(Response.Status.BAD_REQUEST, "Missing required FORMAT parameter");
  }
  ImageRenderer renderer;
  try
  {
    renderer = (ImageRenderer) ImageHandlerFactory.getHandler(format,
        ImageRenderer.class);
  }
  catch (Exception e)
  {
    log.error("Exception thrown", e);
    return writeError(Response.Status.BAD_REQUEST, e.getMessage());
  }
  try
  {
    MrGeoRaster result = renderer.renderImage(layerNames[0], bounds, providerProperties, srs);
    result = colorRaster(layerNames[0],
        (styleNames != null && styleNames.length > 0) ? styleNames[0] : null,
        format, renderer, result, providerProperties);

    Response.ResponseBuilder builder = ((ImageResponseWriter) ImageHandlerFactory
        .getHandler(format, ImageResponseWriter.class))
        .write(result, layerNames[0], bounds);
    return setupCaching(builder, allParams).build();
  }
  catch (IllegalAccessException | InstantiationException | WmsGeneratorException | ImageRendererException e)
  {
    log.error("Unable to render the image in getMosaic", e);
    return writeError(Response.Status.BAD_REQUEST, e.getMessage());
  }
}

/*
 * GetTile implementation
 */
private Response getTile(MultivaluedMap<String, String> allParams,
    final ProviderProperties providerProperties)
{
  String versionStr = getQueryParam(allParams, "version", "1.4.0");
  version = new Version(versionStr);
  if (version.isLess("1.4.0"))
  {
    return writeError(Response.Status.BAD_REQUEST, "Get tile is only supported with version >= 1.4.0");
  }

  String layer = getQueryParam(allParams, "layer");
  if (layer == null || layer.isEmpty())
  {
    return writeError(Response.Status.BAD_REQUEST, "Missing required LAYER parameter");
  }
  String style = getQueryParam(allParams, "style");
  String format = getQueryParam(allParams, "format");
  if (format == null)
  {
    return writeError(Response.Status.BAD_REQUEST, "Missing required FORMAT parameter");
  }
  int tileRow;
  if (paramExists(allParams, "tilerow"))
  {
    tileRow = getQueryParamAsInt(allParams, "tilerow", -1);
  }
  else
  {
    return writeError(Response.Status.BAD_REQUEST, "Missing required TILEROW parameter");
  }
  int tileCol;
  if (paramExists(allParams, "tilecol"))
  {
    tileCol = getQueryParamAsInt(allParams, "tilecol", -1);
  }
  else
  {
    return writeError(Response.Status.BAD_REQUEST, "Missing required TILECOL parameter");
  }
  double scale;
  if (paramExists(allParams, "scale"))
  {
    scale = getQueryParamAsDouble(allParams, "scale", 0.0);
  }
  else
  {
    return writeError(Response.Status.BAD_REQUEST, "Missing required SCALE parameter");
  }

  final ImageRenderer renderer;
  try
  {
    renderer = (ImageRenderer) ImageHandlerFactory.getHandler(format,
        ImageRenderer.class);
  }
  catch (Exception e)
  {
    log.error("Exception thrown", e);
    return writeError(Response.Status.BAD_REQUEST, e.getMessage());
  }
  try
  {
    MrGeoRaster result = renderer.renderImage(layer, tileCol, tileRow, scale, providerProperties);

    result = colorRaster(layer, style, format, renderer, result, providerProperties);

    Response.ResponseBuilder builder = ((ImageResponseWriter) ImageHandlerFactory
        .getHandler(format, ImageResponseWriter.class))
        .write(result, tileCol, tileRow, scale,
            MrsPyramid.open(layer, providerProperties));
    return setupCaching(builder, allParams).build();
  }
  catch (IOException | ImageRendererException | IllegalAccessException | InstantiationException e)
  {
    log.error("Unable to render the image in getTile", e);
    return writeError(Response.Status.BAD_REQUEST, e.getMessage());
  }
}

/*
 * DescribeTiles implementation
 */
private Response describeTiles(String baseURI,
    MultivaluedMap<String, String> allParams,
    final ProviderProperties providerProperties)
{
  String versionStr = getQueryParam(allParams, "version", "1.4.0");
  version = new Version(versionStr);
  if (version.isLess("1.4.0"))
  {
    return writeError(Response.Status.BAD_REQUEST, "Describe tiles is only supported with version >= 1.4.0");
  }

  try
  {
    final DescribeTilesDocumentGenerator docGen = new DescribeTilesDocumentGenerator();
    final Document doc = docGen.generateDoc(version, baseURI,
        RequestUtils.getPyramidFilesList(providerProperties));

    DOMSource source = new DOMSource(doc);
    return Response.ok(source, MediaType.APPLICATION_XML).build();
  }
  catch (IOException e)
  {
    log.error("Exception thrown", e);
    return writeError(Response.Status.BAD_REQUEST, e.getMessage());
  }
}

/*
 * GetCapabilities implementation
 */
private Response getCapabilities(String baseURI, MultivaluedMap<String, String> allParams,
    ProviderProperties providerProperties)
{
  Version version =  new Version(getQueryParam(allParams, "version", "1.1.1"));
  // conform to the version negotiation standards of WMS.
  if (version.isLess("1.3.0"))
  {
    version = new Version("1.1.1");
  }
  else if (version.isLess("1.4.0"))
  {
    version = new Version("1.3.0");
  }
  else
  {
    version = new Version("1.4.0");
  }

  allParams = RequestUtils.replaceParam("VERSION", version.toString(), allParams);

  final GetCapabilitiesDocumentGenerator docGen = new GetCapabilitiesDocumentGenerator();
  try
  {
    // The following code re-builds the request URI to include in the GetCapabilities
    // output. It sorts the parameters so that they are included in the URI in a
    // predictable order. The reason for this is so that test cases can compare XML
    // golden files against the XML generated here without worrying about parameters
    // shifting locations in the URI.
    final Document doc = docGen.generateDoc(version, baseURI, allParams,
        RequestUtils.getPyramidFilesList(providerProperties));

    DOMSource source = new DOMSource(doc);
    return Response.ok(source, MediaType.APPLICATION_XML).build();
  }
  catch (InterruptedException | ParserConfigurationException | IOException e)
  {
    log.error("Exception thrown", e);
    return writeError(Response.Status.BAD_REQUEST, e.getMessage());
  }
}

/*
 * Writes OGC spec error messages to the response
 */
@SuppressWarnings("squid:S1166") // Exception caught and handled
private Response writeError(Response.Status httpStatus, final String msg)
{
  try
  {
    Document doc;
    final DocumentBuilderFactory dBF = DocumentBuilderFactory.newInstance();
    final DocumentBuilder builder = dBF.newDocumentBuilder();
    doc = builder.newDocument();

    final Element ser = doc.createElement("ServiceExceptionReport");
    doc.appendChild(ser);
    ser.setAttribute("version", WMS_VERSION);
    final Element se = XmlUtils.createElement(ser, "ServiceException");
    CDATASection msgNode = doc.createCDATASection(msg);
    se.appendChild(msgNode);

    DOMSource source = new DOMSource(doc);
    return Response.status(httpStatus).entity(source).build();
  }
  catch (ParserConfigurationException ignored)
  {
  }
  // Fallback in case there is an XML exception above
  return Response.status(httpStatus).entity(msg).build();
}

public static class WmsGeneratorException extends IOException
{
  private static final long serialVersionUID = 1L;

  WmsGeneratorException()
  {
    super();
  }

  WmsGeneratorException(final String msg)
  {
    super(msg);
  }

  WmsGeneratorException(final String msg, final Throwable cause)
  {
    super(msg, cause);
  }

  WmsGeneratorException(final Throwable cause)
  {
    super(cause);
  }
}
}
