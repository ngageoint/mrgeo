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

package org.mrgeo.resources;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.ProviderProperties;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.image.MrsPyramid;
import org.mrgeo.services.Configuration;
import org.mrgeo.utils.tms.Bounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.inject.Singleton;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Comparator;

@Path("/kml")
@Singleton
public class KmlGenerator
{

private static final String KML_MIME_TYPE = "application/vnd.google-earth.kml+xml";
private static final Logger log = LoggerFactory.getLogger(KmlGenerator.class);
private final String WMS_VERSION = "1.3.0";
private String baseUrl;
private Version version;

public KmlGenerator()
{
}

public static String getKmlBodyAsString(String service, String url, Bounds inputBounds, String layer, String wmsHost,
    String levelStr, String res, final ProviderProperties providerProperties) throws IOException
{
  String bbox = "";
  if (layer == null)
  {
    throw new IllegalArgumentException("Layer must be specified.");
  }
  double minX = inputBounds.w;
  double minY = inputBounds.s;
  double maxX = inputBounds.e;
  double maxY = inputBounds.n;

  MrsImageDataProvider dp =
      DataProviderFactory.getMrsImageDataProvider(layer, DataProviderFactory.AccessMode.READ, providerProperties);
  MrsPyramid pyramid = MrsPyramid.open(dp);

  int level = pyramid.getMaximumLevel();

  Bounds bounds = pyramid.getBounds();
  if (bounds.toEnvelope().contains(inputBounds.toEnvelope()))
  {
    double layerMinX = bounds.w;
    double layerMinY = bounds.s;
    double layerMaxX = bounds.e;
    double layerMaxY = bounds.n;
    bbox = layerMinX + "," + layerMinY + "," + layerMaxX + "," + layerMaxY;
  }

  String kmlBody;

  //String format = KML_MIME_TYPE;

  String hrefKmlString = "<href>" + url + "?LAYERS=" + layer + "&amp;SERVICE=" + service
      + "&amp;REQUEST=getkmlnode" + "&amp;WMSHOST=" + wmsHost + "&amp;BBOX=" + bbox
      + "&amp;LEVELS=" + level + "&amp;RESOLUTION=" + res + "&amp;NODEID=0,0"
      + "</href>";

  kmlBody = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      + "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n" + "<Document>\n" + "<Region>\n"
      + "  <LatLonAltBox>\n" + "    <north>"
      + String.valueOf(maxY)
      + "</north>\n"
      + "    <south>"
      + String.valueOf(minY)
      + "</south>\n"
      + "    <east>"
      + String.valueOf(maxX)
      + "</east>\n"
      + "    <west>"
      + String.valueOf(minX)
      + "</west>\n"
      + "  </LatLonAltBox>\n"
      + "  <Lod>\n"
      + "    <minLodPixels>256</minLodPixels>\n"
      + "    <maxLodPixels>-1</maxLodPixels>\n"
      + "  </Lod>\n"
      + "</Region>\n"
      + "<NetworkLink>\n"
      + "  <name>0</name>\n"
      + "  <Region>\n"
      + "    <LatLonAltBox>\n"
      + "      <north>"
      + String.valueOf(maxY)
      + "</north>\n"
      + "      <south>"
      + String.valueOf(minY)
      + "</south>\n"
      + "      <east>"
      + String.valueOf(maxX)
      + "</east>\n"
      + "      <west>"
      + String.valueOf(minX)
      + "</west>\n"
      + "   </LatLonAltBox>\n"
      + "   <Lod>\n"
      + "     <minLodPixels>256</minLodPixels>\n"
      + "     <maxLodPixels>-1</maxLodPixels>\n"
      + "   </Lod>\n"
      + " </Region>\n"
      + " <Link>\n"
      + hrefKmlString
      + "\n"
      + "   <viewRefreshMode>onRegion</viewRefreshMode>\n"
      + " </Link>\n"
      + "</NetworkLink>\n" + "</Document>\n" + "</kml>\n";
  return kmlBody;
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
    // original code looked for a MrsPyramid.toc file, not sure why, it wasn't used anywhere else...
//      Path toc = new Path(f.getPath(), "MrsPyramid.toc");
//      if (fs.exists(toc))
//      {
//        files.add(f);
//      }

    providers[i] = DataProviderFactory.getMrsImageDataProvider(images[i],
        DataProviderFactory.AccessMode.READ, providerProperties);
  }

  return providers;
}

public Element createElement(Element parent, String tagName)
{
  Document doc = parent.getOwnerDocument();
  Element e = doc.createElement(tagName);
  parent.appendChild(e);
  return e;
}

public Element createTextElement(Element parent, String tagName, String text)
{
  Document doc = parent.getOwnerDocument();
  Element e = doc.createElement(tagName);
  e.appendChild(doc.createTextNode(text));
  parent.appendChild(e);
  return e;
}

@SuppressWarnings("squid:S1148") // We _are_ printing the exception, to a printwriter!
@SuppressFBWarnings(value = {"SERVLET_QUERY_STRING",
    "SERVLET_PARAMETER"}, justification = "1) QueryString only checked for not null, 2) SERVICE & REQUEST validated")
// @GET
public void doGet(@Context HttpServletRequest request, @Context HttpServletResponse response)
    throws ServletException, IOException
{
  init();

  response.setHeader("Cache-Control", "no-store");
  response.setHeader("Pragma", "no-cache");
  response.setDateHeader("Expires", 0);

  try
  {
    if (request.getQueryString() != null)
    {
      String serviceParam = request.getParameter("SERVICE");
      if (serviceParam == null || serviceParam.isEmpty())
      {
        serviceParam = "kml";
      }

      if (!serviceParam.toLowerCase().equals("kml"))
      {
        throw new KmlGeneratorException("Unsupported service type was requested. (only KML is supported '"
            + serviceParam + "')");
      }

      String requestParam = request.getParameter("REQUEST");
      if (requestParam != null)
      {
        ProviderProperties providerProperties = new ProviderProperties(); // null;
        if (requestParam.toLowerCase().equals("getcapabilities"))
        {
          try
          {
            getCapabilities(request, response, providerProperties);
          }
          catch (TransformerException | ParserConfigurationException e)
          {
            throw new KmlGeneratorException(e);
          }
        }
        else if (requestParam.toLowerCase().equals("getkmlrootnode"))
        {
          getNetworkKmlRootNode(serviceParam, request, response, providerProperties);
        }
        else if (requestParam.toLowerCase().equals("getkmlnode"))
        {
          getNetworkKmlNode(serviceParam, request, response);
        }
        else
        {
          throw new KmlGeneratorException("No viable request made.");
        }
      }
      else
      {
        throw new KmlGeneratorException("No viable request made.");
      }
    }
  }
  catch (KmlGeneratorException e)
  {
    log.error("Exception thrown", e);
    try
    {
      Document doc;
      DocumentBuilderFactory dBF = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = dBF.newDocumentBuilder();
      doc = builder.newDocument();

      Element ser = doc.createElement("ServiceExceptionReport");
      doc.appendChild(ser);
      ser.setAttribute("version", WMS_VERSION);
      Element se = createElement(ser, "ServiceException");
      String code = e.getLocalizedMessage();
      if (code == null || code.isEmpty())
      {
        code = e.getClass().getName();
      }
      se.setAttribute("code", code);
      ByteArrayOutputStream strm = new ByteArrayOutputStream();
      e.printStackTrace(new PrintStream(strm));
      se.setAttribute("locator", strm.toString());
      PrintWriter out = response.getWriter();

      writeDocument(doc, out);
    }
    catch (ParserConfigurationException | TransformerException e1)
    {
      throw new IOException("Exception while creating XML exception (ah, the irony)."
          + e1.getLocalizedMessage(), e1);
    }
//    catch (Exception exception)
//    {
//      throw new IOException("Exception while creating XML exception (ah, the irony)."
//          + exception.getLocalizedMessage());
//    }
  }
}

// @POST
public void doPost(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException
{
  doGet(request, response);
}

private void addHttpElement(Element parent, HttpServletRequest request)
{
  Element http = createElement(parent, "HTTP");
  Element get = createElement(http, "Get");
  if (version.isEqual("1.1.1"))
  {
    Element onlineResource = createElement(get, "OnlineResource");
    onlineResource.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
    onlineResource.setAttribute("xlink:type", "simple");
    onlineResource.setAttribute("xlink:href", request.getRequestURL().toString());
  }
  else
  {
    createTextElement(get, "OnlineResource", request.getRequestURL().toString());
  }

  Element post = createElement(http, "Post");
  if (version.isEqual("1.1.1"))
  {
    Element onlineResource = createElement(post, "OnlineResource");
    onlineResource.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
    onlineResource.setAttribute("xlink:type", "simple");
    onlineResource.setAttribute("xlink:href", request.getRequestURL().toString());
  }
  else
  {
    createTextElement(get, "OnlineResource", request.getRequestURL().toString());
  }
}

/*
 * Checks the provided document against the 1.1.1 DTD
 */
//private void checkForErrors(Document doc) throws SAXException, IOException, TransformerException,
//    ParserConfigurationException
//{
//  ByteArrayOutputStream strm = new ByteArrayOutputStream();
//  PrintWriter pw = new PrintWriter(strm);
//  writeDocument(doc, pw);
//
//  DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
//  factory.setValidating(true);
//  factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
//  DocumentBuilder builder = factory.newDocumentBuilder();
//  builder.setErrorHandler(new KMLErrorHandler());
//
//  ByteArrayInputStream input = new ByteArrayInputStream(strm.toByteArray());
//
//  Document xmlDocument = builder.parse(input);
//  DOMSource source = new DOMSource(xmlDocument);
//  StreamResult result = new StreamResult(new ByteArrayOutputStream());
//  TransformerFactory tf = TransformerFactory.newInstance();
//  Transformer transformer = tf.newTransformer();
//  transformer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM,
//      "http://schemas.opengis.net/wms/1.1.1/WMS_MS_Capabilities.dtd");
//  transformer.transform(source, result);
//}

/*
 *
 */
@SuppressWarnings("squid:S1166") // Exception caught and handled
private void addLayersToCapability(Element capability, Version kmlVersion,
    final ProviderProperties providerProperties) throws IOException
{

  MrsImageDataProvider[] providers = getPyramidFilesList(providerProperties);

  Arrays.sort(providers, new MrsImageComparator());


  Element rootLayer = createElement(capability, "Layer");
  rootLayer.setAttribute("queryable", "0");
  rootLayer.setAttribute("opaque", "0");
  rootLayer.setAttribute("noSubsets", "0");
  createTextElement(rootLayer, "Title", "AllLayers");
  createTextElement(rootLayer, "SRS", "EPSG:4326");

  double minx = Double.MAX_VALUE;
  double maxx = -Double.MAX_VALUE;
  double miny = Double.MAX_VALUE;
  double maxy = -Double.MAX_VALUE;

  for (MrsImageDataProvider f : providers)
  {
    Element layer = createElement(rootLayer, "Layer");
    layer.setAttribute("queryable", "1");
    layer.setAttribute("cascaded", "0");
    layer.setAttribute("opaque", "1");
    layer.setAttribute("noSubsets", "0");
    layer.setAttribute("fixedWidth", "0");
    layer.setAttribute("fixedHeight", "0");
    createTextElement(layer, "Title", f.getResourceName());
    createTextElement(layer, "Name", f.getResourceName());

    try
    {
      MrsPyramid mp = MrsPyramid.open(f);

      minx = Math.min(minx, mp.getBounds().w);
      miny = Math.min(miny, mp.getBounds().s);
      maxx = Math.max(maxx, mp.getBounds().e);
      maxy = Math.max(maxy, mp.getBounds().n);

      if (kmlVersion.isLess("1.3.0"))
      {
        createTextElement(layer, "SRS", "EPSG:4326");
        Element llbb = createElement(layer, "LatLonBoundingBox");
        llbb.setAttribute("minx", String.valueOf(mp.getBounds().w));
        llbb.setAttribute("miny", String.valueOf(mp.getBounds().s));
        llbb.setAttribute("maxx", String.valueOf(mp.getBounds().e));
        llbb.setAttribute("maxy", String.valueOf(mp.getBounds().n));
      }
      else
      {
        createTextElement(layer, "CRS", "CRS:84");
        Element bb = createElement(layer, "EX_GeographicBoundingBox");
        createTextElement(bb, "westBoundLongitude", String.valueOf(mp.getBounds().w));
        createTextElement(bb, "eastBoundLongitude", String.valueOf(mp.getBounds().e));
        createTextElement(bb, "southBoundLatitude", String.valueOf(mp.getBounds().s));
        createTextElement(bb, "northBoundLatitude", String.valueOf(mp.getBounds().n));
      }
    }
    catch (IOException ignored)
    {
      // no op
    }

  }
}

/*
 *
 */
@SuppressFBWarnings(value = "SERVLET_PARAMETER", justification = "VERSION validated")
private void getCapabilities(HttpServletRequest request, HttpServletResponse response,
    final ProviderProperties providerProperties) throws ParserConfigurationException, IOException, TransformerException
{
  Document doc;
  DocumentBuilderFactory dBF = DocumentBuilderFactory.newInstance();
  dBF.setValidating(true);

  DocumentBuilder builder = dBF.newDocumentBuilder();
  doc = builder.newDocument();

  String versionStr = request.getParameter("VERSION");
  if (versionStr == null || versionStr.isEmpty())
  {
    versionStr = "1.1.1";
  }
  version = new Version(versionStr);

  Element wmc = doc.createElement("WMT_MS_Capabilities");
  doc.appendChild(wmc);
  wmc.setAttribute("version", versionStr);
  wmc.setAttribute("updateSequence", "0");
  if (version.isEqual("1.3.0"))
  {
    wmc.setAttribute("xmlns", "http://www.opengis.net/wms");
    wmc.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
    wmc.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
    wmc.setAttribute("xsi:schemaLocation",
        "http://www.opengis.net/wms http://schemas.opengis.net/wms/1.3.0/capabilities_1_3_0.xsd");
  }
  // //
  // Service
  // //
  Element service = createElement(wmc, "Service");
  wmc.appendChild(service);
  // WMT Defined
  createTextElement(service, "Name", "OGC:WMS");
  createTextElement(service, "Title", "MrGeo Web Map Service");
  createTextElement(service, "Abstract", "MrGeo Web Map Service");
  if (version.isLess("1.3.0"))
  {
    // / TODO Take out ASDF
    Element onlineResource = createElement(service, "OnlineResource");
    onlineResource.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
    onlineResource.setAttribute("xlink:type", "simple");
    onlineResource.setAttribute("xlink:href", request.getRequestURL().toString());
  }
  else
  {
    createTextElement(service, "OnlineResource", request.getRequestURL().toString());
  }
  createTextElement(service, "Fees", "none");
  createTextElement(service, "AccessConstraints", "none");

  // //
  // Capability
  // //
  Element capability = createElement(wmc, "Capability");

  // Request
  Element requestTag = createElement(capability, "Request");
  // GetCapabilities
  Element getCapabilities = createElement(requestTag, "GetCapabilities");
  createTextElement(getCapabilities, "Format", "application/vnd.ogc.wms_xml");
  Element gcDcpType = createElement(getCapabilities, "DCPType");
  addHttpElement(gcDcpType, request);

  // GetMap
  Element getMap = createElement(requestTag, "GetKml");
  createTextElement(getMap, "Format", KML_MIME_TYPE);
  Element gmDcpType = createElement(getMap, "DCPType");
  addHttpElement(gmDcpType, request);

  // not supported at this time.
  // // GetFeatureInfo

  // Exception
  Element exception = createElement(capability, "Exception");
  createTextElement(exception, "Format", "application/vnd.ogc.se_xml");
  createTextElement(exception, "Format", "application/vnd.ogc.se_inimage");
  createTextElement(exception, "Format", "application/vnd.ogc.se_blank");

  // Layer
  addLayersToCapability(capability, version, providerProperties);

  PrintWriter out = response.getWriter();

  // checkForErrors(doc);
  writeDocument(doc, out);
}

/*
 *
 */
@SuppressFBWarnings(value = "SERVLET_PARAMETER", justification = "BBOX, NODEID, LEVELS, RESOLUTION validated, LAYERS validated though other request")
private void getNetworkKmlNode(String serviceParam, HttpServletRequest request, HttpServletResponse response)
    throws IOException
{
  String url = request.getRequestURL().toString();
  String bboxParam = request.getParameter("BBOX");
  String nodeIdParam = request.getParameter("NODEID");
  String levelParam = request.getParameter("LEVELS");
  String resParam = request.getParameter("RESOLUTION");

  int maxLevels = Integer.parseInt(levelParam);
  if (maxLevels < 1 || maxLevels >= 22)
  {
    throw new IllegalArgumentException("Levels must be between 1 and 22 inclusive.");
  }

  int resolution = Integer.parseInt(resParam);
  if (resolution < 1 || resolution >= 10000)
  {
    throw new IllegalArgumentException("Resolution must be between 1 and 10000 (pixels) inclusive.");
  }


  String[] bBoxValues = bboxParam.split(",");
  if (bBoxValues.length != 4)
  {
    throw new IllegalArgumentException("Bounding box must have four comma delimited arguments.");
  }

  double minX = Double.parseDouble(bBoxValues[0]);
  double minY = Double.parseDouble(bBoxValues[1]);
  double maxX = Double.parseDouble(bBoxValues[2]);
  double maxY = Double.parseDouble(bBoxValues[3]);

  String[] nodeIdValues = nodeIdParam.split(",");
  if (nodeIdValues.length != 2)
  {
    throw new IllegalArgumentException("Level must have two comma delimited arguments.");
  }

  int levelId = Integer.parseInt(nodeIdValues[0]);
  int nodeId = Integer.parseInt(nodeIdValues[1]);

  String hostParam = getWmsHost(request);

  String layerType = request.getParameter("LAYERS");
  if (layerType == null)
  {
    throw new IllegalArgumentException("Layer type must be specified.");
  }

  StringBuilder kmlBody = new StringBuilder();

  String format = KML_MIME_TYPE;

  double aspect = (maxX - minX) / (maxY - minY);
  int width, height;

  if (aspect > 1.0)
  {
    width = resolution;
    height = (int) (resolution / aspect);
  }
  else
  {
    width = (int) (resolution * aspect);
    height = resolution;
  }

  String hrefImgString = "<href>" + baseUrl + "WmsGenerator" + "?LAYERS=" + layerType
      + "&amp;BBOX=" + String.valueOf(minX) + "," + String.valueOf(minY) + ","
      + String.valueOf(maxX) + "," + String.valueOf(maxY) + "&amp;FORMAT=image/png&amp;WIDTH="
      + String.valueOf(width) + "&amp;" + "HEIGHT=" + String.valueOf(height)
      + "&amp;REQUEST=getMap&amp;SERVICE=wms</href>";

  int minLodPix, maxLodPix;

  if (levelId == 0)
  {
    minLodPix = -1;
    maxLodPix = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
  }
  else if (levelId == maxLevels)
  {
    minLodPix = 256;
    maxLodPix = -1;
  }
  else
  {
    minLodPix = 256;
    maxLodPix = MrGeoConstants.MRGEO_MRS_TILESIZE_DEFAULT_INT;
  }

  kmlBody.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n" +
      "<Document>\n" + " <Region>\n" + "   <LatLonAltBox>\n" + "    <north>")
      .append(String.valueOf(maxY))
      .append("</north>\n")
      .append("    <south>")
      .append(String.valueOf(minY))
      .append("</south>\n")
      .append("    <east>")
      .append(String.valueOf(maxX))
      .append("</east>\n")
      .append("    <west>")
      .append(String.valueOf(minX))
      .append("</west>\n")
      .append("    <rotation>0</rotation>\n")
      .append("   </LatLonAltBox>\n")
      .append("   <Lod>\n")
      .append("    <minLodPixels>")
      .append(String.valueOf(minLodPix))
      .append("</minLodPixels>\n")
      .append("	   <maxLodPixels>")
      .append(String.valueOf(maxLodPix))
      .append("</maxLodPixels>\n")
      .append("	   <minFadeExtent>0</minFadeExtent>\n")
      .append("	   <maxFadeExtent>0</maxFadeExtent>\n")
      .append("   </Lod>\n")
      .append(" </Region>\n");

  int childCounter = 0;
  int childId;

  for (double y = minY; y < maxY && levelId <= maxLevels; y += ((maxY - minY) / 2.0))
  {
    for (double x = minX; x < maxX; x += ((maxX - minX) / 2.0))
    {
      childId = nodeId * 2 + childCounter;

      double childMinX, childMinY, childMaxX, childMaxY;
      childMinX = x;
      childMaxX = x + ((maxX - minX) / 2.0);
      childMinY = y;
      childMaxY = y + ((maxY - minY) / 2.0);

      String childHrefKmlString = url + "?LAYERS=" + layerType + "&amp;SERVICE=" + serviceParam
          + "&amp;REQUEST=getkmlnode" + "&amp;WMSHOST=" + hostParam + "&amp;BBOX="
          + String.valueOf(childMinX) + "," + String.valueOf(childMinY) + ","
          + String.valueOf(childMaxX) + "," + String.valueOf(childMaxY) + "&amp;LEVELS="
          + levelParam + "&amp;RESOLUTION=" + resParam + "&amp;NODEID="
          + String.valueOf(levelId + 1) + "," + String.valueOf(childId);

      kmlBody.append(writeKmlNetworkLink(childMinX, childMinY, childMaxX, childMaxY, levelId,
          childId, childHrefKmlString));

      childCounter++;
    }
  }

  kmlBody.append(" <GroundOverlay>\n" + "   <drawOrder>0</drawOrder>\n" + "   <Icon>\n")
      .append(hrefImgString)
      .append("\n")
      .append("  </Icon>\n")
      .append(" <LatLonBox>\n")
      .append("   <north>")
      .append(String.valueOf(maxY))
      .append("</north>\n")
      .append("   <south>")
      .append(String.valueOf(minY))
      .append("</south>\n")
      .append("   <east>")
      .append(String.valueOf(maxX))
      .append("</east>\n")
      .append("   <west>")
      .append(String.valueOf(minX))
      .append("</west>\n")
      .append("   <minAltitude>0</minAltitude>\n")
      .append("   <maxAltitude>0</maxAltitude>\n")
      .append(" </LatLonBox>\n")
      .append(" </GroundOverlay>\n")
      .append("</Document>\n")
      .append("</kml>\n");

  try (PrintStream kmlStream = new PrintStream(response.getOutputStream()))
  {
    kmlStream.println(kmlBody.toString());
    response.setContentType(format);

    response.setContentLength(kmlBody.length());
  }
}

/*
 *
 */
@SuppressFBWarnings(value = "SERVLET_PARAMETER", justification = "BBOX, LEVELS, RESOLUTION validated, LAYERS validated though other request")
private void getNetworkKmlRootNode(String serviceParam, HttpServletRequest request,
    HttpServletResponse response, final ProviderProperties providerProperties)
    throws IOException
{
  String url = request.getRequestURL().toString();
  String bboxParam = request.getParameter("BBOX");
  String levelParam = request.getParameter("LEVELS");
  String resParam = request.getParameter("RESOLUTION");
  String layer = request.getParameter("LAYERS");

  String headerInfo = "attachment; filename=" + layer.replaceAll("\r\n|\n|\r", "_") + ".kml";
  response.setHeader("Content-Disposition", headerInfo);

  String hostParam = getWmsHost(request);
  String format = KML_MIME_TYPE;

  int maxLevels = Integer.parseInt(levelParam);
  if (maxLevels < 1 || maxLevels >= 22)
  {
    throw new IllegalArgumentException("Levels must be between 1 and 22 inclusive.");
  }

  int resolution = Integer.parseInt(resParam);
  if (resolution < 1 || resolution >= 10000)
  {
    throw new IllegalArgumentException("Resolution must be between 1 and 10000 (pixels) inclusive.");
  }


  String[] bBoxValues = bboxParam.split(",");
  if (bBoxValues.length != 4)
  {
    throw new IllegalArgumentException("Bounding box must have four comma delimited arguments.");
  }

  double minX = Double.parseDouble(bBoxValues[0]);
  double minY = Double.parseDouble(bBoxValues[1]);
  double maxX = Double.parseDouble(bBoxValues[2]);
  double maxY = Double.parseDouble(bBoxValues[3]);

  Bounds inputBounds = new Bounds(minX, minY, maxX, maxY);

  String kmlBody =
      getKmlBodyAsString(serviceParam, url, inputBounds, layer, hostParam, levelParam, resParam, providerProperties);

  try (PrintStream kmlStream = new PrintStream(response.getOutputStream()))
  {

    kmlStream.println(kmlBody);
    response.setContentType(format);

    response.setContentLength(kmlBody.length());

  }
}

/*
 *
 */
@SuppressFBWarnings(value = "SERVLET_PARAMETER", justification = "WMSHOST - simply returning the value sent in")
private String getWmsHost(HttpServletRequest request) throws MalformedURLException
{
  String result = request.getParameter("WMSHOST");
  if (result == null)
  {
    URL requestUrl = new URL(request.getRequestURL().toString());
    result = requestUrl.getHost();
    int port = requestUrl.getPort();
    if (port != -1)
    {
      result = String.format("%s:%d", result, port);
    }
  }
  return result;
}

//private void getKmz(HttpServletRequest request, HttpServletResponse response) throws IOException
//{
//  String url = request.getRequestURL().toString();
//  String serviceParam = request.getParameter("SERVICE");
//
//  String hostParam = getWmsHost(request);
//
//  String layerType = request.getParameter("LAYERS");
//  if (layerType == null)
//  {
//    throw new IllegalArgumentException("Layer type must be specified.");
//  }
//
//  String kmlBody;
//
//  String format = KML_MIME_TYPE;
//
//  String hrefString = "<href>" + url + "?LAYERS=" + layerType + "&amp;SERVICE=" + serviceParam
//      + "&amp;REQUEST=getviewkml" + "&amp;WMSHOST=" + hostParam + "</href>";
//
//  kmlBody = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
//      + "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n" + " <NetworkLink>\n" + "  <name>"
//      + layerType + "Link</name>\n" + "  <open>1</open>\n" + "  <Url>\n" + "   " + hrefString
//      + "   <viewRefreshMode>onStop</viewRefreshMode>\n"
//      + "   <viewRefreshTime>1</viewRefreshTime>\n" + "  </Url>\n"
//      + "  <visibility>1</visibility>\n" + " </NetworkLink>\n" + "</kml>\n";
//
//  ZipOutputStream zos = new ZipOutputStream(response.getOutputStream());
//  zos.setLevel(9);
//
//  try (PrintStream kmlStream = new PrintStream(zos))
//  {
//
//    kmlStream.println(kmlBody);
//    response.setContentType(format);
//
//    response.setContentLength(kmlBody.length());
//  }
//
//}

private void init() throws ServletException
{
  try
  {
    baseUrl = Configuration.getInstance().getProperties().getProperty("base.url");
  }
  catch (IllegalStateException e)
  {
    throw new ServletException("Error reading configuration information.", e);
  }
}

private void writeDocument(Document doc, PrintWriter out) throws TransformerException
{
  TransformerFactory transformerFactory = TransformerFactory.newInstance();
  Transformer transformer = transformerFactory.newTransformer();
  transformer.setOutputProperty(OutputKeys.INDENT, "yes");
  transformer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM,
      "http://schemas.opengis.net/wms/1.1.1/WMS_MS_Capabilities.dtd");
  DOMSource source = new DOMSource(doc);
  StreamResult result = new StreamResult(out);
  transformer.transform(source, result);
}

//  public static String getKmlBodyAsString(String service, String url, Bounds inputBounds, String layer, String wmsHost,
//    String levelStr, String res, final ProviderProperties providerProperties) throws IOException {
//    String bbox="";
//    if (layer == null)
//    {
//      throw new IllegalArgumentException("Layer must be specified.");
//    }
//    double minX = inputBounds.w;
//    double minY = inputBounds.s;
//    double maxX = inputBounds.e;
//    double maxY = inputBounds.n;
//
//    String baseDir = HadoopUtils.getDefaultImageBaseDirectory();
//    Path filePath = new Path(baseDir, layer);
//    int level = 0;
//    FileSystem fs = HadoopFileUtils.getFileSystem(filePath);
//    if (fs.exists(filePath))
//    {
//      MrsPyramid  pyramid = MrsPyramid.open(filePath.toString(), providerProperties);
//      if (pyramid != null)
//      {
//        level = pyramid.getNumLevels();
//        if (levelStr == null)
//        {
//          levelStr = String.valueOf(level);
//        }
//        Bounds bounds = pyramid.getBounds();
//        if (bounds.toEnvelope().contains(inputBounds.toEnvelope()))
//        {
//          double layerMinX = bounds.w;
//          double layerMinY = bounds.s;
//          double layerMaxX = bounds.e;
//          double layerMaxY = bounds.n;
//          bbox = layerMinX + "," + layerMinY + "," + layerMaxX + "," + layerMaxY;
//        }
//      }
//    }
//
//    String kmlBody;
//
//    //String format = KML_MIME_TYPE;
//
//    String hrefKmlString = "<href>" + url + "?LAYERS=" + layer + "&amp;SERVICE=" + service
//        + "&amp;REQUEST=getkmlnode" + "&amp;WMSHOST=" + wmsHost + "&amp;BBOX=" + bbox
//        + "&amp;LEVELS=" + level + "&amp;RESOLUTION=" + res + "&amp;NODEID=0,0"
//        + "</href>";
//
//    kmlBody = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
//        + "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n" + "<Document>\n" + "<Region>\n"
//        + "  <LatLonAltBox>\n" + "    <north>"
//        + String.valueOf(maxY)
//        + "</north>\n"
//        + "    <south>"
//        + String.valueOf(minY)
//        + "</south>\n"
//        + "    <east>"
//        + String.valueOf(maxX)
//        + "</east>\n"
//        + "    <west>"
//        + String.valueOf(minX)
//        + "</west>\n"
//        + "  </LatLonAltBox>\n"
//        + "  <Lod>\n"
//        + "    <minLodPixels>256</minLodPixels>\n"
//        + "    <maxLodPixels>-1</maxLodPixels>\n"
//        + "  </Lod>\n"
//        + "</Region>\n"
//        + "<NetworkLink>\n"
//        + "  <name>0</name>\n"
//        + "  <Region>\n"
//        + "    <LatLonAltBox>\n"
//        + "      <north>"
//        + String.valueOf(maxY)
//        + "</north>\n"
//        + "      <south>"
//        + String.valueOf(minY)
//        + "</south>\n"
//        + "      <east>"
//        + String.valueOf(maxX)
//        + "</east>\n"
//        + "      <west>"
//        + String.valueOf(minX)
//        + "</west>\n"
//        + "   </LatLonAltBox>\n"
//        + "   <Lod>\n"
//        + "     <minLodPixels>256</minLodPixels>\n"
//        + "     <maxLodPixels>-1</maxLodPixels>\n"
//        + "   </Lod>\n"
//        + " </Region>\n"
//        + " <Link>\n"
//        + hrefKmlString
//        + "\n"
//        + "   <viewRefreshMode>onRegion</viewRefreshMode>\n"
//        + " </Link>\n"
//        + "</NetworkLink>\n" + "</Document>\n" + "</kml>\n";
//    return kmlBody;
//  }

private String writeKmlNetworkLink(double minX, double minY, double maxX, double maxY, int level,
    int id, String hrefContent)
{
  String nodeString = "";

  nodeString += " <NetworkLink>\n" + "   <name>" + String.valueOf(level) + "_"
      + String.valueOf(id) + "</name>\n" + "   <Region>\n" + "     <LatLonAltBox>\n"
      + "       <north>" + String.valueOf(maxY) + "</north>\n" + "       <south>"
      + String.valueOf(minY) + "</south>\n" + "       <east>" + String.valueOf(maxX)
      + "</east>\n" + "       <west>" + String.valueOf(minX) + "</west>\n"
      + "       <minAltitude>0</minAltitude>\n" + "       <maxAltitude>0</maxAltitude>\n"
      + "     </LatLonAltBox>\n" + "   <Lod>\n" + "	  <minLodPixels>256</minLodPixels>\n"
      + "	  <maxLodPixels>-1</maxLodPixels>\n" + "	  <minFadeExtent>0</minFadeExtent>\n"
      + "	  <maxFadeExtent>0</maxFadeExtent>\n" + "   </Lod>\n" + "   </Region>\n"
      + "   <Link>\n" + "     <href>" + hrefContent + "</href>\n"
      + "     <viewRefreshMode>onRegion</viewRefreshMode>\n" + "   </Link>\n"
      + " </NetworkLink>\n";

  return nodeString;
}

public static class KmlGeneratorException extends java.lang.Exception
{

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance of <code>DbaseException</code> without detail
   * message.
   */
  public KmlGeneratorException()
  {
  }

  /**
   * Constructs an instance of <code>DbaseException</code> with the specified
   * detail message.
   *
   * @param msg the detail message.
   */
  public KmlGeneratorException(String msg)
  {
    super(msg);
  }

  /**
   * Constructs an instance of <code>DbaseException</code> with the specified
   * detail message and throwable cause.
   *
   * @param msg       the detail message.
   * @param throwable the throwable cause.
   */
  public KmlGeneratorException(String msg, Throwable throwable)
  {
    super(msg, throwable);
  }

  /**
   * Constructs an instance of <code>DbaseException</code> with the specified
   * throwable cause.
   *
   * @param throwable the throwable cause.
   */
  public KmlGeneratorException(Throwable throwable)
  {
    super(throwable);
  }
}

private static class KMLErrorHandler implements ErrorHandler
{
  // Validation errors
  @Override
  public void error(SAXParseException ex) throws SAXParseException
  {
    log.error("Error at " + ex.getLineNumber() + " line.");
    log.error(ex.getMessage());
  }

  // Ignore the fatal errors
  @Override
  public void fatalError(SAXParseException ex) throws SAXException
  {
    log.error("Fatal Error at " + ex.getLineNumber() + " line.");
    log.error(ex.getMessage());
  }

  // Show warnings
  @Override
  public void warning(SAXParseException ex) throws SAXParseException
  {
    log.warn("Warning at " + ex.getLineNumber() + " line.");
    log.warn(ex.getMessage());
  }
}

@SuppressFBWarnings(value = "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE", justification = "Do not need serialization")
private static class MrsImageComparator implements Comparator<MrsImageDataProvider>
{
  @Override
  public int compare(MrsImageDataProvider o1, MrsImageDataProvider o2)
  {
    return o1.getResourceName().compareTo(o2.getResourceName());
  }
}

private static class Version
{
  private int major, minor, micro;

  public Version(String str)
  {
    String[] l = str.split("\\.");
    major = Integer.parseInt(l[0]);
    minor = Integer.parseInt(l[1]);
    micro = Integer.parseInt(l[2]);
  }

  public int compareTo(String str)
  {
    Version other = new Version(str);
    if (other.major < major)
    {
      return 1;
    }
    if (other.major > major)
    {
      return -1;
    }
    if (other.minor < minor)
    {
      return 1;
    }
    if (other.minor > minor)
    {
      return -1;
    }
    if (other.micro < micro)
    {
      return 1;
    }
    if (other.micro > micro)
    {
      return -1;
    }
    return 0;
  }

  public int getMajor()
  {
    return major;
  }

  public int getMicro()
  {
    return micro;
  }

  public int getMinor()
  {
    return minor;
  }

  public boolean isEqual(String str)
  {
    return compareTo(str) == 0;
  }

  public boolean isLess(String str)
  {
    return compareTo(str) == -1;
  }
}
}
