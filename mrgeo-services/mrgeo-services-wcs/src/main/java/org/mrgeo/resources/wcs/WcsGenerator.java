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

package org.mrgeo.resources.wcs;

import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.services.SecurityUtils;
import org.mrgeo.services.Version;
import org.mrgeo.services.mrspyramid.rendering.ImageHandlerFactory;
import org.mrgeo.services.mrspyramid.rendering.ImageRenderer;
import org.mrgeo.services.mrspyramid.rendering.ImageResponseWriter;
import org.mrgeo.services.utils.DocumentUtils;
import org.mrgeo.services.utils.RequestUtils;
import org.mrgeo.services.wcs.DescribeCoverageDocumentGenerator;
import org.mrgeo.services.wcs.WcsCapabilities;
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
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/wcs")
public class WcsGenerator
{
private static final Logger log = LoggerFactory.getLogger(WcsGenerator.class);

public static final String WCS_VERSION = "1.1.0";
private static final String WCS_SERVICE = "wcs";

private Version version = new Version(WCS_VERSION);

private static Map<Version, Document> capabilities = new HashMap<>();
private static UriInfo baseURI = null;

static {
  if (MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_WCS_CAPABILITIES_CACHE, "true").equals("true"))
  {
    new Thread()
    {
      public void run()
      {
        long sleeptime = 60 * 1000 *
            Integer.parseInt(MrGeoProperties.getInstance().getProperty(MrGeoConstants.MRGEO_WCS_CAPABILITIES_REFRESH, "5"));

        boolean stop = false;
        while (!stop)
        {
          try
          {
            for (Version version : capabilities.keySet())
            {
              try
              {
                log.info("refreshing capabilities for version {}", version);
                ProviderProperties providerProperties = SecurityUtils.getProviderProperties();
                Document doc = generateCapabilities(version, baseURI, providerProperties);
                capabilities.put(version, doc);
              }
              catch (ParserConfigurationException | IOException e)
              {
                e.printStackTrace();
              }
            }
            Thread.sleep(sleeptime);
          }
          catch (InterruptedException e)
          {
            e.printStackTrace();
            stop = true;
          }
        }
      }
    }.start();
  }

}

@GET
public Response doGet(@Context UriInfo uriInfo)
{
  log.info("GET URI: {}", uriInfo.getRequestUri().toString());
  return handleRequest(uriInfo);
}

@POST
public Response doPost(@Context UriInfo uriInfo)
{
  log.info("POST URI: {}", uriInfo.getRequestUri().toString());
  return handleRequest(uriInfo);
}

private Response handleRequest(UriInfo uriInfo)
{
  long start = System.currentTimeMillis();

  baseURI = uriInfo;

  MultivaluedMap<String, String> allParams = uriInfo.getQueryParameters();
  String request = getQueryParam(allParams, "request", "GetCapabilities");
  ProviderProperties providerProperties = SecurityUtils.getProviderProperties();

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
    //if (log.isDebugEnabled())
    {
      log.info("WCS request time: {}ms", (System.currentTimeMillis() - start));
      // this can be resource intensive.
      System.gc();
      final Runtime rt = Runtime.getRuntime();
      log.info(String.format("WMS request memory: %.1fMB / %.1fMB\n", (rt.totalMemory() - rt
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
    final ProviderProperties providerProperties)
{
  String versionStr = getQueryParam(allParams, "version", WCS_VERSION);
  version = new Version(versionStr);
  String[] layers = null;
  if (version.isLess("1.1.0"))
  {
    String layer = getQueryParam(allParams, "coverage");
    if (layer == null)
    {
      return writeError(Response.Status.BAD_REQUEST, "Missing required COVERAGE parameter");
    }
    layers = new String[]{layer};
  }
  else
  {
    String layerStr = getQueryParam(allParams, "identifiers");
    if (layerStr == null)
    {
      return writeError(Response.Status.BAD_REQUEST, "Missing required IDENTIFIERS parameter");
    }
    layers = layerStr.split(",");
  }

  try
  {
    final DescribeCoverageDocumentGenerator docGen = new DescribeCoverageDocumentGenerator();
    final Document doc = docGen.generateDoc(version, uriInfo.getRequestUri().toString(), layers);

    ByteArrayOutputStream xmlStream = new ByteArrayOutputStream();
    final PrintWriter out = new PrintWriter(xmlStream);
    // DocumentUtils.checkForErrors(doc);
    DocumentUtils.writeDocument(doc, version, WCS_SERVICE, out);
    out.close();
    return Response.ok(xmlStream.toString()).type(MediaType.APPLICATION_XML).build();
  }
  catch (Exception e)
  {
    return writeError(Response.Status.BAD_REQUEST, e.getMessage());
  }
}


private Response getCapabilities(UriInfo uriInfo, MultivaluedMap<String, String> allParams,
    ProviderProperties providerProperties)
{
  // The versionParamName will be null if the request did not include the
  // version parameter.
  String acceptVersions = getQueryParam(allParams, "acceptversions", null);
  Version version = null;
  if (acceptVersions != null)
  {
    String[] versions = acceptVersions.split(",");

    for (String ver: versions)
    {
      if (version == null || version.isLess(ver))
      {
        version = new Version(ver);
      }
    }
  }
  else {
    version = new Version(getQueryParam(allParams, "version", WCS_VERSION));
  }

  try
  {
    Document doc;
    if (capabilities.containsKey(version))
    {
      log.warn("*** cached!");
      doc = capabilities.get(version);
    }
    else
    {
      log.warn("*** NOT cached!");

      doc = generateCapabilities(version, uriInfo, providerProperties);
      capabilities.put(version, doc);
    }

    ByteArrayOutputStream xmlStream = new ByteArrayOutputStream();
    final PrintWriter out = new PrintWriter(xmlStream);
    // DocumentUtils.checkForErrors(doc);
    DocumentUtils.writeDocument(doc, version, WCS_SERVICE, out);
    out.close();
    return Response.ok(xmlStream.toString()).type(MediaType.APPLICATION_XML).build();
  }
  catch (Exception e)
  {
    return writeError(Response.Status.BAD_REQUEST, e.getMessage());
  }


//    return writeError(Response.Status.BAD_REQUEST, "Not Implemented");
}

private static Document generateCapabilities(Version version, UriInfo uriInfo, ProviderProperties providerProperties)
    throws IOException, ParserConfigurationException, InterruptedException
{
  final WcsCapabilities docGen = new WcsCapabilities();

  // The following code re-builds the request URI to include in the GetCapabilities
  // output. It sorts the parameters so that they are included in the URI in a
  // predictable order. The reason for this is so that test cases can compare XML
  // golden files against the XML generated here without worrying about parameters
  // shifting locations in the URI.
//  Set<String> keys = uriInfo.getQueryParameters().keySet();
//  String[] sortedKeys = new String[keys.size()];
//  keys.toArray(sortedKeys);
//  Arrays.sort(sortedKeys);

  UriBuilder builder = uriInfo.getBaseUriBuilder().path(uriInfo.getPath());

  return docGen.generateDoc(version, builder.build().toString() + "?",
      getPyramidFilesList(providerProperties));
}


/*
 * Returns a list of all MrsPyramid version 2 data in the home data directory
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
        DataProviderFactory.AccessMode.READ,
        providerProperties);
  }

  return providers;
}

private Response getCoverage(MultivaluedMap<String, String> allParams,
    ProviderProperties providerProperties)
{
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
        return writeError(Response.Status.BAD_REQUEST, e.getMessage());
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
  catch (Exception e)
  {
    return writeError(Response.Status.BAD_REQUEST, e.getMessage());
  }

  // Return the resulting image
  try
  {
    log.info("Rendering " + layer);
    Raster result = renderer.renderImage(layer, bounds, width, height, providerProperties, crs);

    log.info("Generating response");
    Response.ResponseBuilder builder = ((ImageResponseWriter) ImageHandlerFactory
        .getHandler(format, ImageResponseWriter.class))
        .write(result, layer, bounds);

    log.info("Building and returning response");
    return builder.build();
  }
  catch (Exception e)
  {
    log.error("Unable to render the image in getCoverage", e);
    return writeError(Response.Status.BAD_REQUEST, e.getMessage());
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
