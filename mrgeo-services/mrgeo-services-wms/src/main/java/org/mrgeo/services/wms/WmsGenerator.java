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

package org.mrgeo.services.wms;

import java.awt.image.Raster;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Semaphore;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.lang.StringUtils;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.rasterops.ColorScale;
import org.mrgeo.rasterops.ColorScale.ColorScaleException;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.services.SecurityUtils;
import org.mrgeo.services.ServletUtils;
import org.mrgeo.services.Version;
import org.mrgeo.services.mrspyramid.ColorScaleManager;
import org.mrgeo.services.mrspyramid.rendering.ColorScaleApplier;
import org.mrgeo.services.mrspyramid.rendering.ImageHandlerFactory;
import org.mrgeo.services.mrspyramid.rendering.ImageRenderer;
import org.mrgeo.services.mrspyramid.rendering.ImageResponseWriter;
import org.mrgeo.services.mrspyramid.rendering.TiffImageRenderer;
import org.mrgeo.services.utils.RequestUtils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.LatLng;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.XmlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

//import org.mrgeo.utils.LoggingUtils;

/**
 * OGC WMS implementation - See https://107.23.31.196/redmine/projects/mrgeo/wiki/WmsReference for
 * details.
 */
public class WmsGenerator extends HttpServlet
{
  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(WmsGenerator.class);

  //  private static Path basePath = null;
//  private static Path colorScaleBasePath = null;
  private static Semaphore semaphore = null;

  // private static CoordinateReferenceSystem coordSys = null;

  public static final String JPEG_MIME_TYPE = "image/jpeg";
  public static final String PNG_MIME_TYPE = "image/png";
  public static final String TIFF_MIME_TYPE = "image/tiff";

  private Version version = null;
  public static final String WMS_VERSION = "1.3.0";

  public WmsGenerator() throws NumberFormatException
  {
    super();
    if (semaphore == null)
    {
      semaphore = new Semaphore(Integer.parseInt(MrGeoProperties.getInstance().getProperty(
          "org.mrgeo.services.WmsGenerator.permits", "3")), true);
    }
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


  /*
   * GetMap implementation
   */
  private static void getMap(final HttpServletRequest request,
      final HttpServletResponse response,
      final Properties providerProperties)
      throws Exception
  {
//    final Path filePath = getLayerPath(ServletUtils.validateAndGetParamValue(request, "layers",
//        "string"));
    final String layer = ServletUtils.validateAndGetParamValue(request, "layers",
        "string");
    final int width = Integer.valueOf(ServletUtils.validateAndGetParamValue(request, "width",
        "integer"));
    final int height = Integer.valueOf(ServletUtils.validateAndGetParamValue(request, "height",
        "integer"));
    Bounds bounds = RequestUtils.boundsFromParam(ServletUtils.validateAndGetParamValue(
        request, "bbox", "string"));

    final String style = ServletUtils.getParamValue(request, "style");
    final String srs = ServletUtils.getParamValue(request, "srs");

    final String imageFormat = ServletUtils.getParamValue(request, "format");

    final ImageRenderer renderer = (ImageRenderer) ImageHandlerFactory.getHandler(imageFormat,
        ImageRenderer.class);

      //Reproject bounds to EPSG:4326 if necessary
      bounds = RequestUtils.reprojectBounds(bounds, srs);

    Raster result = renderer.renderImage(layer, bounds, width, height, providerProperties, srs);

    result = colorRaster(layer, style, imageFormat, renderer, result);

    ((ImageResponseWriter) ImageHandlerFactory.getHandler(imageFormat, ImageResponseWriter.class))
        .write(result, layer, bounds, response);
  }

  /*
   * GetMosaic implementation
   */
  private static void
  getMosaic(final HttpServletRequest request, final HttpServletResponse response,
      final Properties providerProperties)
      throws Exception
  {
    final String layer = ServletUtils.validateAndGetParamValue(request, "layers",
        "string");
    final Bounds bounds = RequestUtils.boundsFromParam(ServletUtils.validateAndGetParamValue(
        request, "bbox", "string"));
    final String style = ServletUtils.getParamValue(request, "style");
    final String srs = ServletUtils.getParamValue(request, "srs");

    final String imageFormat = ServletUtils.getParamValue(request, "format");

    final ImageRenderer renderer = (ImageRenderer) ImageHandlerFactory.getHandler(imageFormat,
        ImageRenderer.class);
    Raster result = renderer.renderImage(layer, bounds, providerProperties, srs);

    result = colorRaster(layer, style, imageFormat, renderer, result);

    ((ImageResponseWriter) ImageHandlerFactory.getHandler(imageFormat, ImageResponseWriter.class))
        .write(result, layer, bounds, response);
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
      final Properties providerProperties) throws IOException
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
  private static void getTile(final HttpServletRequest request,
      final HttpServletResponse response,
      final Properties providerProperties)
      throws Exception
  {
    final String layer = ServletUtils.validateAndGetParamValue(request, "layer",
        "string");
    final int tileRow = Integer.valueOf(ServletUtils.validateAndGetParamValue(request, "tilerow",
        "int"));
    final int tileCol = Integer.valueOf(ServletUtils.validateAndGetParamValue(request, "tilecol",
        "int"));
    final double scale = Double.valueOf(ServletUtils.validateAndGetParamValue(request, "scale",
        "double"));
    final String style = ServletUtils.getParamValue(request, "style");

    final String imageFormat = ServletUtils.getParamValue(request, "format");

    final ImageRenderer renderer = (ImageRenderer) ImageHandlerFactory.getHandler(imageFormat,
        ImageRenderer.class);
    // TODO: Need to construct provider properties from the WebRequest using
    // a new security layer and pass those properties.
    Raster result = renderer.renderImage(layer, tileCol, tileRow, scale, (Properties)null);

    result = colorRaster(layer, style, imageFormat, renderer, result);

    ((ImageResponseWriter) ImageHandlerFactory.getHandler(imageFormat, ImageResponseWriter.class))
        .write(result, tileCol, tileRow, scale,
            MrsImagePyramid.open(layer, providerProperties),
            response);
  }

  private static Raster colorRaster(String layer, String style, String imageFormat, ImageRenderer renderer,
      Raster result) throws Exception
  {
    if (!(renderer instanceof TiffImageRenderer))
    {
      log.debug("Applying color scale to image {} ...", layer);

      ColorScale cs;
      if (style != null)
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

//  /**
//   * Sets up the home data directory
//   */
//  @Override
//  public void init(final ServletConfig conf) throws ServletException
//  {
//    super.init(conf);
//    try
//    {
//      if (basePath == null)
//      {
//        basePath = new Path(Configuration.getInstance().getProperties().getProperty("image.base"));
//      }
//      System.out.println("image base path: " + basePath.toString());
//      log.debug("image base path: {}", basePath.toString());
//      if (colorScaleBasePath == null)
//      {
//        colorScaleBasePath = new Path(Configuration.getInstance().getProperties().getProperty(
//          "colorscale.base"));
//      }
//      System.out.println("color scale base path: " + colorScaleBasePath.toString());
//    }
//    catch (final IllegalStateException e)
//    {
//      throw new ServletException("image.base must be specified in the MrGeo configuration file (" +
//        e.getMessage() + ")");
//    }
//  }

  /**
   * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
   */
  @Override
  protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
      throws ServletException, IOException
  {
    final long start = System.currentTimeMillis();
    try
    {
      log.debug("Semaphores available: {}", semaphore.availablePermits());
      semaphore.acquire();
      log.debug("Semaphore acquired.  Semaphores available: {}", semaphore.availablePermits());

      ServletUtils.printRequestURL(request);
      ServletUtils.printRequestAttributes(request);
      ServletUtils.printRequestParams(request);

      final String cache = ServletUtils.getParamValue(request, "cache");
      if (!StringUtils.isEmpty(cache) && cache.toLowerCase().equals("off"))
      {
        response.setHeader("Cache-Control", "no-store");
        response.setHeader("Pragma", "no-cache");
        response.setDateHeader("Expires", 0);
      }
      else
      {
        response.setHeader("Cache-Control", "max-age=3600");
        response.setHeader("Pragma", "");
        response.setDateHeader("Expires", 3600);
      }

      String requestParam = ServletUtils.getParamValue(request, "request");
      if (requestParam == null || requestParam.isEmpty())
      {
        requestParam = "GetCapabilities";
      }
      requestParam = requestParam.toLowerCase();

      String serviceParam = ServletUtils.getParamValue(request, "service");
      if (serviceParam == null || serviceParam.isEmpty())
      {
        serviceParam = "wms";
      }
      if (!serviceParam.toLowerCase().equals("wms"))
      {
        throw new Exception("Invalid service type was requested. (only WMS is supported '" +
            serviceParam + "')");
      }

      if (requestParam.equals("getmap") || requestParam.equals("getmosaic") ||
          requestParam.equals("gettile"))
      {
        if (!requestParam.equals("gettile"))
        {
          ServletUtils.validateParam(request, "layers", "string");
        }
        else
        {
          ServletUtils.validateParam(request, "layer", "string");
        }
        ServletUtils.validateParam(request, "format", "string");
        final String cs = ServletUtils.getParamValue(request, "crs");
        if (!StringUtils.isEmpty(cs))
        {
          if (!cs.toUpperCase().equals("CRS:84"))
          {
            throw new Exception("InvalidCRS: Invalid coordinate system \"" + cs +
                "\".  Only coordinate system CRS:84 is supported.");
          }
        }

        OpImageRegistrar.registerMrGeoOps();
      }

      // TODO: Need to construct provider properties from the WebRequest using
      // a new security layer and pass those properties to MapAlgebraJob.
      Properties providerProperties = SecurityUtils.getProviderProperties();
      if (requestParam.equals("getcapabilities"))
      {
        getCapabilities(request, response, providerProperties);
      }
      else if (requestParam.equals("getmap"))
      {
        getMap(request, response, providerProperties);
      }
      else if (requestParam.equals("getmosaic"))
      {
        getMosaic(request, response, providerProperties);
      }
      else if (requestParam.equals("gettile"))
      {
        getTile(request, response, providerProperties);
      }
      else if (requestParam.equals("describetiles"))
      {
        describeTiles(request, response, providerProperties);
      }
      else
      {
        throw new Exception("Invalid request type made.");
      }
    }
    catch (final Exception e)
    {
      e.printStackTrace();
      try
      {
        response.setContentType("text/xml");
        writeError(e, response);
      }
      // we already started writing out to HTTP, instead return an error.
      catch (final Exception exception)
      {
        log.warn("Exception writing error: {}", exception);
        throw new IOException("Exception while writing XML exception (ah, the irony). " +
            "Original Exception is below." + exception.getLocalizedMessage(), e);
      }
    }
    finally
    {
      semaphore.release();

      if (log.isDebugEnabled())
      {
        log.debug("Semaphore released.  Semaphores available: {}", semaphore.availablePermits());
        log.debug("WMS request time: {}ms", (System.currentTimeMillis() - start));
        // this can be resource intensive.
        System.gc();
        final Runtime rt = Runtime.getRuntime();
        log.debug(String.format("WMS request memory: %.1fMB / %.1fMB\n", (rt.totalMemory() - rt
            .freeMemory()) / 1e6, rt.maxMemory() / 1e6));
      }
    }
  }

  /**
   * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
   */
  @Override
  protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
      throws ServletException, IOException
  {
    doGet(request, response);
  }

  /*
   * DescribeTiles implementation
   */
  private void describeTiles(final HttpServletRequest request,
      final HttpServletResponse response,
      final Properties providerProperties)
      throws IOException, ServletException, TransformerException
  {
    String versionStr = ServletUtils.getParamValue(request, "version");
    if (versionStr == null || versionStr.isEmpty())
    {
      versionStr = "1.4.0";
    }
    version = new Version(versionStr);
    if (version.isLess("1.4.0"))
    {
      throw new ServletException("Describe tiles is only supported with version >= 1.4.0");
    }

    final DescribeTilesDocumentGenerator docGen = new DescribeTilesDocumentGenerator();
    final Document doc = docGen.generateDoc(version, request.getRequestURL().toString(),
        getPyramidFilesList(providerProperties));

    final PrintWriter out = response.getWriter();
    // DocumentUtils.checkForErrors(doc);
    DocumentUtils.writeDocument(doc, version, out);
  }

  /*
   * GetCapabilities implementation
   */
  private void
  getCapabilities(final HttpServletRequest request, final HttpServletResponse response,
      final Properties providerProperties)
      throws Exception
  {
    String versionStr = ServletUtils.getParamValue(request, "version");
    if (versionStr == null || versionStr.isEmpty())
    {
      versionStr = "1.1.1";
    }
    version = new Version(versionStr);
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
    final Document doc = docGen.generateDoc(version, request.getRequestURL().toString(),
        getPyramidFilesList(providerProperties));

    final PrintWriter out = response.getWriter();
    // DocumentUtils.checkForErrors(doc);
    DocumentUtils.writeDocument(doc, version, out);
  }

  /*
   * Writes OGC spec error messages to the response
   */
  private void writeError(final Exception e, final HttpServletResponse response)
      throws ParserConfigurationException, IOException, TransformerException
  {
    response.reset();

    Document doc;
    final DocumentBuilderFactory dBF = DocumentBuilderFactory.newInstance();
    final DocumentBuilder builder = dBF.newDocumentBuilder();
    doc = builder.newDocument();

    final Element ser = doc.createElement("ServiceExceptionReport");
    doc.appendChild(ser);
    ser.setAttribute("version", WMS_VERSION);
    final Element se = XmlUtils.createElement(ser, "ServiceException");
    String code = e.getLocalizedMessage();
    if (code == null || code.isEmpty())
    {
      code = e.getClass().getName();
    }
    se.setAttribute("code", code);
    final ByteArrayOutputStream strm = new ByteArrayOutputStream();
    e.printStackTrace(new PrintStream(strm));
    se.setAttribute("locator", strm.toString());
    final PrintWriter out = response.getWriter();

    DocumentUtils.writeDocument(doc, version, out);
  }
}
