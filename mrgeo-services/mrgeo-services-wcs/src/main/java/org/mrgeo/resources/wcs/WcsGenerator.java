package org.mrgeo.resources.wcs;

import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.services.SecurityUtils;
import org.mrgeo.services.Version;
import org.mrgeo.services.mrspyramid.rendering.ImageHandlerFactory;
import org.mrgeo.services.mrspyramid.rendering.ImageRenderer;
import org.mrgeo.services.mrspyramid.rendering.ImageResponseWriter;
import org.mrgeo.services.utils.DocumentUtils;
import org.mrgeo.services.utils.RequestUtils;
import org.mrgeo.utils.Bounds;
import org.mrgeo.utils.XmlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.CDATASection;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

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
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.Properties;

@Path("/wcs")
public class WcsGenerator
{
  private static final Logger log = LoggerFactory.getLogger(WcsGenerator.class);

  public static final String WCS_VERSION = "1.1.0";
  private static final String WCS_SERVICE = "wms";

  private Version version = new Version(WCS_VERSION);

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

  private Response handleRequest(UriInfo uriInfo)
  {
    long start = System.currentTimeMillis();

    MultivaluedMap<String, String> allParams = uriInfo.getQueryParameters();
    String request = getQueryParam(allParams, "request", "GetCapabilities");
    Properties providerProperties = SecurityUtils.getProviderProperties();

    try
    {
      String serviceName = getQueryParam(allParams, "service");
      if (serviceName == null)
      {
        return writeError(Response.Status.BAD_REQUEST, "Missing required SERVICE parameter. Should be set to \"WCS\"");
      }
      if (!serviceName.equalsIgnoreCase("wcs"))
      {
        return writeError(Response.Status.BAD_REQUEST, "Invalid SERVICE parameter. Should be set to \"WCS\"");
      }

      if (request.equalsIgnoreCase("getcapabilities"))
      {
        return getCapabilities(uriInfo, allParams, providerProperties);
      }
      else if (request.equalsIgnoreCase("describecoverage"))
      {
        return describeCoverage(uriInfo, allParams, providerProperties);
      }
      else if (request.equalsIgnoreCase("getcoverage"))
      {
        return getCoverage(allParams, providerProperties);
      }

      return writeError(Response.Status.BAD_REQUEST, "Invalid request");
    }
    finally
    {
      if (log.isDebugEnabled())
      {
        log.debug("WCS request time: {}ms", (System.currentTimeMillis() - start));
        // this can be resource intensive.
        System.gc();
        final Runtime rt = Runtime.getRuntime();
        log.debug(String.format("WMS request memory: %.1fMB / %.1fMB\n", (rt.totalMemory() - rt
            .freeMemory()) / 1e6, rt.maxMemory() / 1e6));
      }
    }
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

  private Response describeCoverage(UriInfo uriInfo,
      MultivaluedMap<String,String> allParams,
      final Properties providerProperties)
  {
    String versionStr = getQueryParam(allParams, "version", WCS_VERSION);
    version = new Version(versionStr);
    if (version.isLess("1.4.0"))
    {
      return writeError(Response.Status.BAD_REQUEST, "Describe tiles is only supported with version >= 1.4.0");
    }

//    try
//    {
//      final DescribeCoverageDocumentGenerator docGen = new DescribeCoverageDocumentGenerator();
//      final Document doc = docGen.generateDoc(version, uriInfo.getRequestUri().toString(),
//          getPyramidFilesList(providerProperties));
//
//      ByteArrayOutputStream xmlStream = new ByteArrayOutputStream();
//      final PrintWriter out = new PrintWriter(xmlStream);
//      // DocumentUtils.checkForErrors(doc);
//      DocumentUtils.writeDocument(doc, version, out);
//      out.close();
//      return Response.ok(xmlStream.toString()).type(MediaType.APPLICATION_XML).build();
//    }
//    catch (Exception e)
//    {
//      return writeError(Response.Status.BAD_REQUEST, e);
//    }

    return writeError(Response.Status.BAD_REQUEST, "Not Implemented");
  }


  private Response getCapabilities(UriInfo uriInfo, MultivaluedMap<String, String> allParams,
      Properties providerProperties)
  {
    // The versionParamName will be null if the request did not include the
    // version parameter.
    String versionParamName = getActualQueryParamName(allParams, "version");
    String versionStr = getQueryParam(allParams, "version", WCS_VERSION);
    Version version = new Version(versionStr);

//    final GetCapabilitiesDocumentGenerator docGen = new GetCapabilitiesDocumentGenerator();
//    try
//    {
//      // The following code re-builds the request URI to include in the GetCapabilities
//      // output. It sorts the parameters so that they are included in the URI in a
//      // predictable order. The reason for this is so that test cases can compare XML
//      // golden files against the XML generated here without worrying about parameters
//      // shifting locations in the URI.
//      Set<String> keys = uriInfo.getQueryParameters().keySet();
//      String[] sortedKeys = new String[keys.size()];
//      keys.toArray(sortedKeys);
//      Arrays.sort(sortedKeys);
//      UriBuilder builder = uriInfo.getBaseUriBuilder().path(uriInfo.getPath());
//      for (String key : sortedKeys)
//      {
//        // Only include the VERSION parameter in the URI used in GetCapabilities
//        // if it was included in the original URI request.
//        if (key.equalsIgnoreCase("version"))
//        {
//          if (versionParamName != null)
//          {
//            builder = builder.queryParam(versionParamName, versionStr);
//          }
//        }
//        else
//        {
//          builder = builder.queryParam(key, getQueryParam(allParams, key));
//        }
//      }
//      final Document doc = docGen.generateDoc(version, builder.build().toString(),
//          getPyramidFilesList(providerProperties));
//
//      ByteArrayOutputStream xmlStream = new ByteArrayOutputStream();
//      final PrintWriter out = new PrintWriter(xmlStream);
//      // DocumentUtils.checkForErrors(doc);
//      DocumentUtils.writeDocument(doc, version, out);
//      out.close();
//      return Response.ok(xmlStream.toString()).type(MediaType.APPLICATION_XML).build();
//    }
//    catch (Exception e)
//    {
//      return writeError(Response.Status.BAD_REQUEST, e);
//    }

    return writeError(Response.Status.BAD_REQUEST, "Not Implemented");
  }


  private Response getCoverage(MultivaluedMap<String, String> allParams, Properties providerProperties)
  {
    OpImageRegistrar.registerMrGeoOps();

    // Get all of the query parameter values needed and validate them
    String versionStr = getQueryParam(allParams, "version", WCS_VERSION);
    version = new Version(versionStr);

    String layer = null;
    if (version.isLess("1.1.0"))
    {
      layer = getQueryParam(allParams, "coverage");
    }
    else
    {
      layer = getQueryParam(allParams, "identifier");
    }

    if (layer == null)
    {
      return writeError(Response.Status.BAD_REQUEST, "Missing required COVERAGE parameter");
    }


    String crs = null;
    Bounds bounds = new Bounds();
    try
    {
      if (version.isLess("1.1.0"))
      {
        crs = getBoundsParam(allParams, "bbox", bounds);
        try
        {
          crs = getCrsParam(allParams);
        }
        catch (Exception e)
        {
          return writeError(Response.Status.BAD_REQUEST, e);
        }
      }
      else
      {
        crs = getBoundsParam(allParams, "boundingbox", bounds);
      }
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

    int width = getQueryParamAsInt(allParams, "width", -1);
    if (width < 0)
    {
      return writeError(Response.Status.BAD_REQUEST, "Missing required WIDTH parameter");
    }
    else if (width == 0)
    {
      return writeError(Response.Status.BAD_REQUEST, "WIDTH parameter must be greater than 0");
    }

    int height = getQueryParamAsInt(allParams, "height", -1);
    if (height < 0)
    {
      return writeError(Response.Status.BAD_REQUEST, "Missing required HEIGHT parameter");
    }
    else if (height == 0)
    {
      return writeError(Response.Status.BAD_REQUEST, "HEIGHT parameter must be greater than 0");
    }

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
      bounds = RequestUtils.reprojectBounds(bounds, crs);
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
      Raster result = renderer.renderImage(layer, bounds, width, height, providerProperties, crs);

      Response.ResponseBuilder builder = ((ImageResponseWriter) ImageHandlerFactory
          .getHandler(format, ImageResponseWriter.class))
          .write(result, layer, bounds);

      return builder.build();
    }
    catch (Exception e)
    {
      log.error("Unable to render the image in getCoverage", e);
      return writeError(Response.Status.BAD_REQUEST, e);
    }
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

  private String getBoundsParam(MultivaluedMap<String, String> allParams, String paramName, Bounds bounds)
      throws Exception
  {
    String bbox = getQueryParam(allParams, paramName);
    if (bbox == null)
    {
      throw new Exception("Missing required " + paramName.toUpperCase() + " parameter");
    }
    String[] bboxComponents = bbox.split(",");
    String crs = null;
    if (bboxComponents.length == 5)
    {
      crs = bboxComponents[4];
    }
    else if (bboxComponents.length != 4)
    {
      throw new Exception("Invalid \" + paramName.toUpperCase() + \" parameter. Should contain minX, minY, maxX, maxY");
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

    bounds.expand(bboxValues[0], bboxValues[1], bboxValues[2], bboxValues[3]);

    return crs;
  }

  private String getCrsParam(MultivaluedMap<String, String> allParams) throws Exception
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
      ser.setAttribute("version", WCS_VERSION);
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
      DocumentUtils.writeDocument(doc, version, WCS_SERVICE, out);
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
      ser.setAttribute("version", WCS_VERSION);
      final Element se = XmlUtils.createElement(ser, "ServiceException");
      CDATASection msgNode = doc.createCDATASection(msg);
      se.appendChild(msgNode);
      final ByteArrayOutputStream xmlStream = new ByteArrayOutputStream();
      final PrintWriter out = new PrintWriter(xmlStream);
      DocumentUtils.writeDocument(doc, version, WCS_SERVICE, out);
      out.close();
      return Response
          .status(httpStatus)
          .header("Content-Type", MediaType.TEXT_XML)
          .entity(xmlStream.toString())
          .build();
    }
    catch (ParserConfigurationException | TransformerException ignored)
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
      ser.setAttribute("version", WCS_VERSION);
      final Element se = XmlUtils.createElement(ser, "ServiceException");
      se.setAttribute("code", code);
      CDATASection msgNode = doc.createCDATASection(msg);
      se.appendChild(msgNode);
      final ByteArrayOutputStream xmlStream = new ByteArrayOutputStream();
      final PrintWriter out = new PrintWriter(xmlStream);
      DocumentUtils.writeDocument(doc, version, WCS_SERVICE, out);
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
