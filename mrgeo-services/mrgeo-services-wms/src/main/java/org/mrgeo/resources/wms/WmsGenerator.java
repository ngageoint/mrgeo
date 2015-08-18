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

package org.mrgeo.resources.wms;

import org.apache.commons.lang3.StringUtils;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.rasterops.ColorScale;
import org.mrgeo.rasterops.ColorScale.ColorScaleException;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.services.SecurityUtils;
import org.mrgeo.services.Version;
import org.mrgeo.services.mrspyramid.ColorScaleManager;
import org.mrgeo.services.mrspyramid.rendering.*;
import org.mrgeo.services.utils.DocumentUtils;
import org.mrgeo.services.utils.RequestUtils;
import org.mrgeo.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.CDATASection;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.servlet.ServletException;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.awt.image.Raster;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.*;

//import javax.servlet.http.HttpServlet;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;

//import org.mrgeo.utils.LoggingUtils;

/**
 * OGC WMS implementation - See https://107.23.31.196/redmine/projects/mrgeo/wiki/WmsReference for
 * details.
 */
@Path("/wms")
public class WmsGenerator
{
  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(WmsGenerator.class);

  //  private static Path basePath = null;
//  private static Path colorScaleBasePath = null;
  // private static CoordinateReferenceSystem coordSys = null;

  public static final String JPEG_MIME_TYPE = "image/jpeg";
  public static final String PNG_MIME_TYPE = "image/png";
  public static final String TIFF_MIME_TYPE = "image/tiff";

  private Version version = new Version(WMS_VERSION);
  public static final String WMS_VERSION = "1.3.0";
  private static final String WMS_SERVICE = "wms";

  public WmsGenerator()
  {
  }

  /**
   * Calculates the scale of the requested WMS layer
   *
   * @param image
   *          source data
   * @return scale resolution
   */
  public static double calculateScale(final MrsImage image)
  {
    // WMS defines a pixel as .28mm
    final double h = TMSUtils.resolution(image.getZoomlevel(), image.getTilesize()) *
        LatLng.METERS_PER_DEGREE;
    return h / 2.8e-4;
  }

  /**
   * Returns the value for the specified paramName case-insensitively. If the
   * parameter does not exist, it returns null.
   *
   * @param allParams
   * @param paramName
   * @return
   */
  private String getQueryParam(MultivaluedMap<String, String> allParams, String paramName)
  {
    for (String key: allParams.keySet())
    {
      if (key.equalsIgnoreCase(paramName))
      {
        List<String> value = allParams.get(key);
        if (value.size() == 1)
        {
          return value.get(0);
        }
      }
    }
    return null;
  }

  /**
   * Returns the value for the specified paramName case-insensitively. If the
   * parameter does not exist, it returns defaultValue.
   *
   * @param allParams
   * @param paramName
   * @param defaultValue
   * @return
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
    for (String key: allParams.keySet())
    {
      if (key.equalsIgnoreCase(paramName))
      {
        List<String> value = allParams.get(key);
        return (value.size() > 0);
      }
    }
    return false;
  }

  private String getActualQueryParamName(MultivaluedMap<String, String> allParams,
                                         String paramName)
  {
    for (String key: allParams.keySet())
    {
      if (key.equalsIgnoreCase(paramName))
      {
        return key;
      }
    }
    return null;
  }

  /**
   * Returns the int value for the specified paramName case-insensitively. If
   * the parameter value exists, but is not an int, it throws a NumberFormatException.
   * If it does not exist, it returns defaultValue.
   *
   * @param allParams
   * @param paramName
   * @return
   */
  private int getQueryParamAsInt(MultivaluedMap<String, String> allParams,
                                 String paramName,
                                 int defaultValue)
          throws NumberFormatException
  {
    for (String key: allParams.keySet())
    {
      if (key.equalsIgnoreCase(paramName))
      {
        List<String> value = allParams.get(key);
        if (value.size() == 1)
        {
          return Integer.parseInt(value.get(0));
        }
      }
    }
    return defaultValue;
  }

  /**
   * Returns the int value for the specified paramName case-insensitively. If
   * the parameter value exists, but is not an int, it throws a NumberFormatException.
   * If it does not exist, it returns defaultValue.
   *
   * @param allParams
   * @param paramName
   * @return
   */
  private double getQueryParamAsDouble(MultivaluedMap<String, String> allParams,
                                       String paramName,
                                       double defaultValue)
          throws NumberFormatException
  {
    for (String key: allParams.keySet())
    {
      if (key.equalsIgnoreCase(paramName))
      {
        List<String> value = allParams.get(key);
        if (value.size() == 1)
        {
          return Double.parseDouble(value.get(0));
        }
      }
    }
    return defaultValue;
  }

  @GET
  public Response doGet(@Context UriInfo uriInfo)
  {
    return handleRequest(uriInfo);
  }

  @POST
  public Response doPost(@Context UriInfo uriInfo)
  {
    return handleRequest(uriInfo);
  }

  private Response handleRequest(@Context UriInfo uriInfo)
  {
    long start = System.currentTimeMillis();
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
        return getCapabilities(uriInfo, allParams, providerProperties);
      }
      else if (request.equalsIgnoreCase("describetiles"))
      {
        return describeTiles(uriInfo, allParams, providerProperties);
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
        log.debug(String.format("WMS request memory: %.1fMB / %.1fMB\n", (rt.totalMemory() - rt
                .freeMemory()) / 1e6, rt.maxMemory() / 1e6));
      }
    }
  }

  private boolean isCacheOff(MultivaluedMap<String,String> allParams)
  {
    String cacheValue = getQueryParam(allParams, "cache", "");
    return (!StringUtils.isEmpty(cacheValue) && cacheValue.equalsIgnoreCase("off"));
  }

  private Response.ResponseBuilder setupCaching(Response.ResponseBuilder builder,
                                                MultivaluedMap<String,String> allParams)
  {
    boolean cacheOff = isCacheOff(allParams);
    CacheControl cacheControl = new CacheControl();
    if (cacheOff)
    {
      cacheControl.setNoStore(true);
      cacheControl.setNoCache(true);
      // This is retained from the original WmsGenerator, but it seems wrong.
      return builder.cacheControl(cacheControl).expires(new Date(0));
    }
    else
    {
      // This is retained from the original WmsGenerator, but it seems wrong.
      cacheControl.setMaxAge(3600);
      cacheControl.setNoCache(false);
      return builder.cacheControl(cacheControl).expires(new Date(3600));
    }
  }

  private Response getMap(MultivaluedMap<String, String> allParams, ProviderProperties providerProperties)
  {
    OpImageRegistrar.registerMrGeoOps();

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
    String srs = null;
    try
    {
      srs = getSrsParam(allParams);
    }
    catch (Exception e)
    {
      return writeError(Response.Status.BAD_REQUEST, e);
    }
    Bounds bounds = null;
    try
    {
      bounds = getBoundsParam(allParams, "bbox");
    }
    catch (Exception e)
    {
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

    ImageRenderer renderer = null;
    try
    {
      renderer = (ImageRenderer) ImageHandlerFactory.getHandler(format, ImageRenderer.class);
    }
    catch (Exception e)
    {
      return writeError(Response.Status.BAD_REQUEST, e.getMessage());
    }

    // Reproject bounds to EPSG:4326 if necessary
    try
    {
      bounds = RequestUtils.reprojectBounds(bounds, srs);
    }
    catch (org.opengis.referencing.NoSuchAuthorityCodeException e)
    {
      return writeError(Response.Status.BAD_REQUEST, "InvalidCRS", e.getMessage());
    }
    catch (Exception e)
    {
      return writeError(Response.Status.BAD_REQUEST, e);
    }

    // Return the resulting image
    try
    {
      Raster result = renderer.renderImage(layerNames[0], bounds, width, height, providerProperties, srs);

      result = colorRaster(layerNames[0],
          (styleNames != null && styleNames.length > 0) ? styleNames[0] : null,
          format,
          renderer,
          result);

      Response.ResponseBuilder builder = ((ImageResponseWriter) ImageHandlerFactory
              .getHandler(format, ImageResponseWriter.class))
              .write(result, layerNames[0], bounds);
      return setupCaching(builder, allParams).build();
    }
    catch (Exception e)
    {
      log.error("Unable to render the image in getTile", e);
      return writeError(Response.Status.BAD_REQUEST, e);
    }
  }

  /**
   * Gets the value for the paramName from allParams. If the value does not exist, it
   * throws an exception. It then parses the minX, minY, maxX and maxY settings from
   * the value. If any of the settings are not valid or missing, it throws an
   * exception. If all validation passes, it returns a Bounds object configured with
   * those settings.
   *
   * @param allParams
   * @param paramName
   * @return
   * @throws Exception
   */
  private Bounds getBoundsParam(MultivaluedMap<String, String> allParams, String paramName)
          throws Exception
  {
    String bbox = getQueryParam(allParams, paramName);
    if (bbox == null)
    {
      throw new Exception("Missing required BBOX parameter");
    }
    String[] bboxComponents = bbox.split(",");
    if (bboxComponents.length != 4)
    {
      throw new Exception("Invalid BBOX parameter. Should contain minX, minY, maxX, maxY");
    }
    double[] bboxValues = new double[4];
    for (int index=0; index < bboxComponents.length; index++)
    {
      try
      {
        bboxValues[index] = Double.parseDouble(bboxComponents[index]);
      }
      catch (NumberFormatException nfe)
      {
        throw new Exception("Invalid BBOX value: " + bboxComponents[index]);
      }
    }
    return new Bounds(bboxValues[0], bboxValues[1], bboxValues[2], bboxValues[3]);
  }

  private String getSrsParam(MultivaluedMap<String, String> allParams) throws Exception
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
    OpImageRegistrar.registerMrGeoOps();

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
      return writeError(Response.Status.BAD_REQUEST, e);
    }
    String format = getQueryParam(allParams, "format");
    if (format == null)
    {
      return writeError(Response.Status.BAD_REQUEST, "Missing required FORMAT parameter");
    }
    ImageRenderer renderer = null;
    try
    {
      renderer = (ImageRenderer) ImageHandlerFactory.getHandler(format,
                                                                ImageRenderer.class);
    }
    catch (Exception e)
    {
      return writeError(Response.Status.BAD_REQUEST, e);
    }
    try
    {
      Raster result = renderer.renderImage(layerNames[0], bounds, providerProperties, srs);
      result = colorRaster(layerNames[0],
                           (styleNames != null && styleNames.length > 0) ? styleNames[0] : null,
                           format,
                           renderer,
                           result);

      Response.ResponseBuilder builder = ((ImageResponseWriter) ImageHandlerFactory
              .getHandler(format, ImageResponseWriter.class))
              .write(result, layerNames[0], bounds);
      return setupCaching(builder, allParams).build();
    }
    catch (Exception e)
    {
      log.error("Unable to render the image in getMosaic", e);
      return writeError(Response.Status.BAD_REQUEST, e);
    }
  }

  private static ColorScale getDefaultColorScale()
  {
    ColorScale cs = null;
    try
    {
      cs = ColorScaleManager.fromName("Default");
    }
    catch (ColorScaleException e)
    {
      // Do nothing - there may not be a Default color scale defined
    }
    if (cs == null)
    {
      cs = ColorScale.createDefault();
    }
    return cs;
  }

  /*
   * Returns a list of all MrsImagePyramid version 2 data in the home data directory
   */
  private static MrsImageDataProvider[] getPyramidFilesList(
      final ProviderProperties providerProperties) throws IOException
  {
    String[] images = DataProviderFactory.listImages(providerProperties);

    Arrays.sort(images);

    MrsImageDataProvider[] providers = new MrsImageDataProvider[images.length];

    for (int i = 0; i < images.length; i++)
    {
      providers[i] = DataProviderFactory.getMrsImageDataProvider(images[i],
          DataProviderFactory.AccessMode.READ, providerProperties);
    }

    return providers;
//    Path basePath = new Path(HadoopUtils.getDefaultImageBaseDirectory());
//    final FileSystem fileSystem = HadoopFileUtils.getFileSystem(basePath);
//    // log.debug(HadoopFileUtils.getDefaultRoot().toString());
//    // log.debug("basePath: {}", fileSystem.makeQualified(basePath).toString());
//    FileStatus[] allFiles = fileSystem.listStatus(basePath);
//    if (allFiles == null || allFiles.length == 0)
//    {
//      log.warn("Base path either doesn't exist or has no files. {}", basePath.toString());
//      allFiles = new FileStatus[0];
//    }
//
//    final LinkedList<FileStatus> files = new LinkedList<FileStatus>();
//    for (final FileStatus f : allFiles)
//    {
//      if (f.isDir())
//      {
//        final Path metadataPath = new Path(f.getPath(), "metadata");
//        if (fileSystem.exists(metadataPath))
//        {
//          log.debug("Using directory: {}", f.getPath().toString());
//          files.add(f);
//        }
//        else
//        {
//          log.warn("Skipping directory: {}", f.getPath().toString());
//        }
//      }
//      else
//      {
//        log.debug("Skipping file: {}", f.getPath().toString());
//      }
//    }
//    return files;
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

    OpImageRegistrar.registerMrGeoOps();

    String layer = getQueryParam(allParams, "layer");
    if (layer == null || layer.isEmpty())
    {
      return writeError(Response.Status.BAD_REQUEST, "Missing required LAYER parameter");
    }
    String style = getQueryParam(allParams, "style");
//    if (style == null || style.isEmpty())
//    {
//      return writeError(Response.Status.BAD_REQUEST, "Missing required STYLE parameter");
//    }
    String format = getQueryParam(allParams, "format");
    if (format == null)
    {
      return writeError(Response.Status.BAD_REQUEST, "Missing required FORMAT parameter");
    }
    int tileRow = -1;
    if (paramExists(allParams, "tilerow"))
    {
      tileRow = getQueryParamAsInt(allParams, "tilerow", -1);
    }
    else
    {
      return writeError(Response.Status.BAD_REQUEST, "Missing required TILEROW parameter");
    }
    int tileCol = -1;
    if (paramExists(allParams, "tilecol"))
    {
      tileCol = getQueryParamAsInt(allParams, "tilecol", -1);
    }
    else
    {
      return writeError(Response.Status.BAD_REQUEST, "Missing required TILECOL parameter");
    }
    double scale = 0.0;
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
      return writeError(Response.Status.BAD_REQUEST, e);
    }
    try
    {
      Raster result = renderer.renderImage(layer, tileCol, tileRow, scale, providerProperties);

      result = colorRaster(layer, style, format, renderer, result);

      Response.ResponseBuilder builder =  ((ImageResponseWriter) ImageHandlerFactory
              .getHandler(format, ImageResponseWriter.class))
              .write(result, tileCol, tileRow, scale,
                     MrsImagePyramid.open(layer, providerProperties));
      return setupCaching(builder, allParams).build();
    }
    catch (Exception e)
    {
      log.error("Unable to render the image in getTile", e);
      return writeError(Response.Status.BAD_REQUEST, e);
    }
  }

  private static Raster colorRaster(String layer, String style, String imageFormat, ImageRenderer renderer,
      Raster result) throws Exception
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
          throw new ServletException("Can not load style: " + style);
        }
      }
      else
      {
        cs = ColorScale.createDefaultGrayScale();
      }
      result = ((ColorScaleApplier) ImageHandlerFactory.getHandler(imageFormat,
          ColorScaleApplier.class)).applyColorScale(result, cs,renderer.getExtrema(), renderer.getDefaultValues());
      log.debug("Color scale applied to image {}", layer);
    }

    return result;
  }

  /*
   * DescribeTiles implementation
   */
  private Response describeTiles(UriInfo uriInfo,
                                 MultivaluedMap<String,String> allParams,
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
      final Document doc = docGen.generateDoc(version, uriInfo.getRequestUri().toString(),
                                              getPyramidFilesList(providerProperties));

      ByteArrayOutputStream xmlStream = new ByteArrayOutputStream();
      final PrintWriter out = new PrintWriter(xmlStream);
      // DocumentUtils.checkForErrors(doc);
      DocumentUtils.writeDocument(doc, version, WMS_SERVICE, out);
      out.close();
      return Response.ok(xmlStream.toString()).type(MediaType.APPLICATION_XML).build();
    }
    catch (Exception e)
    {
      return writeError(Response.Status.BAD_REQUEST, e);
    }
  }

  /*
   * GetCapabilities implementation
   */
  private Response getCapabilities(UriInfo uriInfo, MultivaluedMap<String, String> allParams,
                                   ProviderProperties providerProperties)
  {
    // The versionParamName will be null if the request did not include the
    // version parameter.
    String versionParamName = getActualQueryParamName(allParams, "version");
    String versionStr = getQueryParam(allParams, "version", "1.1.1");
    Version version = new Version(versionStr);
    // conform to the version negotiation standards of WMS.
    if (version.isLess("1.3.0"))
    {
      versionStr = "1.1.1";
      version = new Version(versionStr);
    }
    else if (version.isLess("1.4.0"))
    {
      versionStr = "1.3.0";
      version = new Version(versionStr);
    }
    else
    {
      versionStr = "1.4.0";
      version = new Version(versionStr);
    }
        
    final GetCapabilitiesDocumentGenerator docGen = new GetCapabilitiesDocumentGenerator();
    try
    {
      // The following code re-builds the request URI to include in the GetCapabilities
      // output. It sorts the parameters so that they are included in the URI in a
      // predictable order. The reason for this is so that test cases can compare XML
      // golden files against the XML generated here without worrying about parameters
      // shifting locations in the URI.
      Set<String> keys = uriInfo.getQueryParameters().keySet();
      String[] sortedKeys = new String[keys.size()];
      keys.toArray(sortedKeys);
      Arrays.sort(sortedKeys);
      UriBuilder builder = uriInfo.getBaseUriBuilder().path(uriInfo.getPath());
      for (String key : sortedKeys)
      {
        // Only include the VERSION parameter in the URI used in GetCapabilities
        // if it was included in the original URI request.
        if (key.equalsIgnoreCase("version"))
        {
          if (versionParamName != null)
          {
            builder = builder.queryParam(versionParamName, versionStr);
          }
        }
        else
        {
          builder = builder.queryParam(key, getQueryParam(allParams, key));
        }
      }
      final Document doc = docGen.generateDoc(version, builder.build().toString(),
                                              getPyramidFilesList(providerProperties));

      ByteArrayOutputStream xmlStream = new ByteArrayOutputStream();
      final PrintWriter out = new PrintWriter(xmlStream);
      // DocumentUtils.checkForErrors(doc);
      DocumentUtils.writeDocument(doc, version, WMS_SERVICE, out);
      out.close();
      return Response.ok(xmlStream.toString()).type(MediaType.APPLICATION_XML).build();
    }
    catch (Exception e)
    {
      return writeError(Response.Status.BAD_REQUEST, e);
    }
  }

  /*
   * Writes OGC spec error messages to the response
   */
  private Response writeError(Response.Status httpStatus, final Exception e)
  {
    try
    {
      Document doc;
      final DocumentBuilderFactory dBF = DocumentBuilderFactory.newInstance();
      final DocumentBuilder builder;
      builder = dBF.newDocumentBuilder();
      doc = builder.newDocument();

      final Element ser = doc.createElement("ServiceExceptionReport");
      doc.appendChild(ser);
      ser.setAttribute("version", WMS_VERSION);
      final Element se = XmlUtils.createElement(ser, "ServiceException");
      String msg = e.getLocalizedMessage();
      if (msg == null || msg.isEmpty())
      {
        msg = e.getClass().getName();
      }
      final ByteArrayOutputStream strm = new ByteArrayOutputStream();
      e.printStackTrace(new PrintStream(strm));
      CDATASection msgNode = doc.createCDATASection(strm.toString());
      se.appendChild(msgNode);
      final ByteArrayOutputStream xmlStream = new ByteArrayOutputStream();
      final PrintWriter out = new PrintWriter(xmlStream);
      DocumentUtils.writeDocument(doc, version, WMS_SERVICE, out);
      out.close();
      return Response
              .status(httpStatus)
              .header("Content-Type", MediaType.TEXT_XML)
              .entity(xmlStream.toString())
              .build();
    }
    catch (ParserConfigurationException e1)
    {
    }
    catch (TransformerException e1)
    {
    }
    // Fallback in case there is an XML exception above
    return Response.status(httpStatus).entity(e.getLocalizedMessage()).build();
  }

  /*
   * Writes OGC spec error messages to the response
   */
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
      final ByteArrayOutputStream xmlStream = new ByteArrayOutputStream();
      final PrintWriter out = new PrintWriter(xmlStream);
      DocumentUtils.writeDocument(doc, version, WMS_SERVICE, out);
      out.close();
      return Response
              .status(httpStatus)
              .header("Content-Type", MediaType.TEXT_XML)
              .entity(xmlStream.toString())
              .build();
    }
    catch (ParserConfigurationException e1)
    {
    }
    catch (TransformerException e1)
    {
    }
    // Fallback in case there is an XML exception above
    return Response.status(httpStatus).entity(msg).build();
  }

  /*
   * Writes OGC spec error messages to the response
   */
  private Response writeError(Response.Status httpStatus, final String code, final String msg)
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
      se.setAttribute("code", code);
      CDATASection msgNode = doc.createCDATASection(msg);
      se.appendChild(msgNode);
      final ByteArrayOutputStream xmlStream = new ByteArrayOutputStream();
      final PrintWriter out = new PrintWriter(xmlStream);
      DocumentUtils.writeDocument(doc, version, WMS_SERVICE, out);
      out.close();
      return Response
              .status(httpStatus)
              .header("Content-Type", MediaType.TEXT_XML)
              .entity(xmlStream.toString())
              .build();
    }
    catch (ParserConfigurationException e1)
    {
    }
    catch (TransformerException e1)
    {
    }
    // Fallback in case there is an XML exception above
    return Response.status(httpStatus).entity(msg).build();
  }
}
